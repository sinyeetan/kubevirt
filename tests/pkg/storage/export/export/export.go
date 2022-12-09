/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright 2022 Red Hat, Inc.
 *
 */

package export

import (
	"context"
	"crypto/rsa"
	"fmt"
	"path"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/rand"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/pointer"

	virtv1 "kubevirt.io/api/core/v1"
	exportv1 "kubevirt.io/api/export/v1alpha1"
	"kubevirt.io/client-go/kubecli"
	"kubevirt.io/client-go/log"
	cdiv1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"

	"kubevirt.io/kubevirt/pkg/certificates/bootstrap"
	"kubevirt.io/kubevirt/pkg/certificates/triple"
	"kubevirt.io/kubevirt/pkg/certificates/triple/cert"
	"kubevirt.io/kubevirt/pkg/controller"
	"kubevirt.io/kubevirt/pkg/virt-operator/resource/generate/components"

	"kubevirt.io/kubevirt/pkg/storage/snapshot"
	"kubevirt.io/kubevirt/pkg/storage/types"
	"kubevirt.io/kubevirt/pkg/virt-controller/services"
	watchutil "kubevirt.io/kubevirt/pkg/virt-controller/watch/util"

	"github.com/openshift/library-go/pkg/build/naming"
	validation "k8s.io/apimachinery/pkg/util/validation"
)

const (
	unexpectedResourceFmt  = "unexpected resource %+v"
	failedKeyFromObjectFmt = "failed to get key from object: %v, %v"
	enqueuedForSyncFmt     = "enqueued %q for sync"

	pvcNotFoundReason  = "PVCNotFound"
	pvcBoundReason     = "PVCBound"
	pvcPendingReason   = "PVCPending"
	unknownReason      = "Unknown"
	initializingReason = "Initializing"
	inUseReason        = "InUse"
	podPendingReason   = "PodPending"
	podReadyReason     = "PodReady"
	podCompletedReason = "PodCompleted"

	exportServiceLabel = "kubevirt.io.virt-export-service"

	exportPrefix = "virt-export"

	blockVolumeMountPath = "/dev/export-volumes"
	fileSystemMountPath  = "/export-volumes"
	urlBasePath          = "/volumes"

	// annContentType is an annotation on a PVC indicating the content type. This is populated by CDI.
	annContentType = "cdi.kubevirt.io/storage.contentType"

	caDefaultPath = "/etc/virt-controller/exportca"
	caCertFile    = caDefaultPath + "/tls.crt"
	caKeyFile     = caDefaultPath + "/tls.key"
	// name of certificate secret volume in pod
	certificates = "certificates"

	exporterPodFailedOrCompletedEvent = "ExporterPodFailedOrCompleted"
	exporterPodCreatedEvent           = "ExporterPodCreated"
	ExportPaused                      = "ExportPaused"
	secretCreatedEvent                = "SecretCreated"
	serviceCreatedEvent               = "ServiceCreated"

	certExpiry = time.Duration(30 * time.Hour) // 30 hours
	deadline   = time.Duration(24 * time.Hour) // 24 hours

	kvm = 107

	requeueTime = time.Second * 3
)

// variable so can be overridden in tests
var currentTime = func() *metav1.Time {
	t := metav1.Now()
	return &t
}

var exportGVK = schema.GroupVersionKind{
	Group:   exportv1.SchemeGroupVersion.Group,
	Version: exportv1.SchemeGroupVersion.Version,
	Kind:    "VirtualMachineExport",
}

var datavolumeGVK = schema.GroupVersionKind{
	Group:   cdiv1.SchemeGroupVersion.Group,
	Version: cdiv1.SchemeGroupVersion.Version,
	Kind:    "DataVolume",
}

func rawURI(pvc *corev1.PersistentVolumeClaim) string {
	return path.Join(fmt.Sprintf("%s/%s/disk.img", urlBasePath, pvc.Name))
}

func rawGzipURI(pvc *corev1.PersistentVolumeClaim) string {
	return path.Join(fmt.Sprintf("%s/%s/disk.img.gz", urlBasePath, pvc.Name))
}

func archiveURI(pvc *corev1.PersistentVolumeClaim) string {
	return path.Join(fmt.Sprintf("%s/%s/disk.tar.gz", urlBasePath, pvc.Name))
}

func dirURI(pvc *corev1.PersistentVolumeClaim) string {
	return path.Join(fmt.Sprintf("%s/%s/dir", urlBasePath, pvc.Name)) + "/"
}

type sourceVolumes struct {
	volumes          []*corev1.PersistentVolumeClaim
	inUse            bool
	isPopulated      bool
	availableMessage string
}

func (sv *sourceVolumes) isSourceAvailable() bool {
	return !sv.inUse && sv.isPopulated
}

// VMExportController is resonsible for exporting VMs
type VMExportController struct {
	Client kubecli.KubevirtClient

	TemplateService services.TemplateService

	VMExportInformer          cache.SharedIndexInformer
	PVCInformer               cache.SharedIndexInformer
	VMSnapshotInformer        cache.SharedIndexInformer
	VMSnapshotContentInformer cache.SharedIndexInformer
	PodInformer               cache.SharedIndexInformer
	DataVolumeInformer        cache.SharedIndexInformer
	ConfigMapInformer         cache.SharedIndexInformer
	ServiceInformer           cache.SharedIndexInformer
	VMInformer                cache.SharedIndexInformer
	VMIInformer               cache.SharedIndexInformer
	RouteConfigMapInformer    cache.SharedInformer
	RouteCache                cache.Store
	IngressCache              cache.Store
	SecretInformer            cache.SharedIndexInformer
	VolumeSnapshotProvider    snapshot.VolumeSnapshotProvider

	Recorder record.EventRecorder

	KubevirtNamespace string
	ResyncPeriod      time.Duration

	vmExportQueue workqueue.RateLimitingInterface

	caCertManager *bootstrap.FileCertificateManager
}

var initCert = func(ctrl *VMExportController) {
	ctrl.caCertManager = bootstrap.NewFileCertificateManager(caCertFile, caKeyFile)
	go ctrl.caCertManager.Start()
}

// Init initializes the export controller
func (ctrl *VMExportController) Init() {
	ctrl.vmExportQueue = workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "virt-controller-export-vmexport")

	ctrl.VMExportInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ctrl.handleVMExport,
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.handleVMExport(newObj) },
		},
		ctrl.ResyncPeriod,
	)
	ctrl.PodInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ctrl.handlePod,
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.handlePod(newObj) },
			DeleteFunc: ctrl.handlePod,
		},
		ctrl.ResyncPeriod,
	)
	ctrl.ServiceInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ctrl.handleService,
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.handleService(newObj) },
			DeleteFunc: ctrl.handleService,
		},
		ctrl.ResyncPeriod,
	)
	ctrl.PVCInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ctrl.handlePVC,
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.handlePVC(newObj) },
			DeleteFunc: ctrl.handlePVC,
		},
		ctrl.ResyncPeriod,
	)
	ctrl.VMSnapshotInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ctrl.handleVMSnapshot,
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.handleVMSnapshot(newObj) },
			DeleteFunc: ctrl.handleVMSnapshot,
		},
		ctrl.ResyncPeriod,
	)
	ctrl.VMIInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ctrl.handleVMI,
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.handleVMI(newObj) },
			DeleteFunc: ctrl.handleVMI,
		},
		ctrl.ResyncPeriod,
	)
	ctrl.VMInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ctrl.handleVM,
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.handleVM(newObj) },
			DeleteFunc: ctrl.handleVM,
		},
		ctrl.ResyncPeriod,
	)

	initCert(ctrl)
}

// Run the controller
func (ctrl *VMExportController) Run(threadiness int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer ctrl.vmExportQueue.ShutDown()

	log.Log.Info("Starting export controller.")
	defer log.Log.Info("Shutting down export controller.")

	if !cache.WaitForCacheSync(
		stopCh,
		ctrl.VMExportInformer.HasSynced,
		ctrl.PVCInformer.HasSynced,
		ctrl.PodInformer.HasSynced,
		ctrl.DataVolumeInformer.HasSynced,
		ctrl.ConfigMapInformer.HasSynced,
		ctrl.ServiceInformer.HasSynced,
		ctrl.RouteConfigMapInformer.HasSynced,
		ctrl.SecretInformer.HasSynced,
		ctrl.VMSnapshotInformer.HasSynced,
		ctrl.VMSnapshotContentInformer.HasSynced,
		ctrl.VMInformer.HasSynced,
		ctrl.VMIInformer.HasSynced,
	) {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	for i := 0; i < threadiness; i++ {
		go wait.Until(ctrl.vmExportWorker, time.Second, stopCh)
	}

	<-stopCh

	return nil
}

func (ctrl *VMExportController) vmExportWorker() {
	for ctrl.processVMExportWorkItem() {
	}
}

func (ctrl *VMExportController) processVMExportWorkItem() bool {
	return watchutil.ProcessWorkItem(ctrl.vmExportQueue, func(key string) (time.Duration, error) {
		log.Log.V(3).Infof("vmExport worker processing key [%s]", key)

		storeObj, exists, err := ctrl.VMExportInformer.GetStore().GetByKey(key)
		if !exists || err != nil {
			return 0, err
		}

		vmExport, ok := storeObj.(*exportv1.VirtualMachineExport)
		if !ok {
			return 0, fmt.Errorf(unexpectedResourceFmt, storeObj)
		}

		return ctrl.updateVMExport(vmExport.DeepCopy())
	})
}

func (ctrl *VMExportController) handlePod(obj interface{}) {
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}

	if pod, ok := obj.(*corev1.Pod); ok {
		key := ctrl.getOwnerVMexportKey(pod)
		_, exists, err := ctrl.VMExportInformer.GetStore().GetByKey(key)
		if err != nil {
			utilruntime.HandleError(err)
			return
		}
		if exists {
			log.Log.V(3).Infof("Adding VMExport due to pod %s", key)
			ctrl.vmExportQueue.Add(key)
		}
	}
}

func (ctrl *VMExportController) handleService(obj interface{}) {
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}

	if service, ok := obj.(*corev1.Service); ok {
		serviceKey := ctrl.getOwnerVMexportKey(service)
		_, exists, err := ctrl.VMExportInformer.GetStore().GetByKey(serviceKey)
		if err != nil {
			utilruntime.HandleError(err)
			return
		}
		if exists {
			log.Log.V(3).Infof("Adding VMExport due to service %s", serviceKey)
			ctrl.vmExportQueue.Add(serviceKey)
		}
	}
}

func (ctrl *VMExportController) getPVCsFromName(namespace, name string) *corev1.PersistentVolumeClaim {
	pvc, exists, err := ctrl.getPvc(namespace, name)
	if err != nil {
		log.Log.V(3).Infof("Error getting pvc by name %v", err)
		return nil
	}
	if exists {
		return pvc
	}
	return nil
}

func (ctrl *VMExportController) updateVMExport(vmExport *exportv1.VirtualMachineExport) (time.Duration, error) {
	log.Log.V(3).Infof("Updating VirtualMachineExport %s/%s", vmExport.Namespace, vmExport.Name)

	if vmExport.DeletionTimestamp != nil {
		return 0, nil
	}

	service, err := ctrl.getOrCreateExportService(vmExport)
	if err != nil {
		return 0, err
	}

	if ctrl.isSourcePvc(&vmExport.Spec) {
		return ctrl.handleSource(vmExport, service, ctrl.getPVCFromSourcePVC, ctrl.updateVMExportPvcStatus)
	}
	if ctrl.isSourceVMSnapshot(&vmExport.Spec) {
		return ctrl.handleSource(vmExport, service, ctrl.getPVCFromSourceVMSnapshot, ctrl.updateVMExporVMSnapshotStatus)
	}
	if ctrl.isSourceVM(&vmExport.Spec) {
		return ctrl.handleSource(vmExport, service, ctrl.getPVCFromSourceVM, ctrl.updateVMExportVMStatus)
	}
	return 0, nil
}

type pvcFromSourceFunc func(*exportv1.VirtualMachineExport) (*sourceVolumes, error)
type updateVMExportStatusFunc func(*exportv1.VirtualMachineExport, *corev1.Pod, *corev1.Service, *sourceVolumes) (time.Duration, error)

func (ctrl *VMExportController) handleSource(vmExport *exportv1.VirtualMachineExport, service *corev1.Service, getPVCFromSource pvcFromSourceFunc, updateStatus updateVMExportStatusFunc) (time.Duration, error) {
	sourceVolumes, err := getPVCFromSource(vmExport)
	if err != nil {
		return 0, err
	}
	log.Log.V(4).Infof("Source volumes %v", sourceVolumes)

	pod, err := ctrl.manageExporterPod(vmExport, sourceVolumes)
	if err != nil {
		return 0, err
	}

	return updateStatus(vmExport, pod, service, sourceVolumes)
}

func (ctrl *VMExportController) manageExporterPod(vmExport *exportv1.VirtualMachineExport, sourceVolumes *sourceVolumes) (*corev1.Pod, error) {
	pod, podExists, err := ctrl.getExporterPod(vmExport)
	if err != nil {
		return nil, err
	}
	if !podExists {
		if sourceVolumes.isSourceAvailable() {
			if len(sourceVolumes.volumes) > 0 {
				pod, err = ctrl.createExporterPod(vmExport, sourceVolumes.volumes)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	if pod != nil {
		if pod.Status.Phase == corev1.PodPending {
			if err := ctrl.getOrCreateCertSecret(vmExport, pod); err != nil {
				return nil, err
			}
		}

		if sourceVolumes.isSourceAvailable() {
			if err := ctrl.handlePodSucceededOrFailed(vmExport, pod); err != nil {
				return nil, err
			}
		} else {
			// source is not available, stop the exporter pod if started
			if err := ctrl.deleteExporterPod(vmExport, pod, ExportPaused, sourceVolumes.availableMessage); err != nil {
				return nil, err
			}
			pod = nil
		}
	}
	return pod, nil
}

func (ctrl *VMExportController) deleteExporterPod(vmExport *exportv1.VirtualMachineExport, pod *corev1.Pod, deleteReason, message string) error {
	ctrl.Recorder.Eventf(vmExport, corev1.EventTypeWarning, deleteReason, message)
	if err := ctrl.Client.CoreV1().Pods(vmExport.Namespace).Delete(context.Background(), pod.Name, metav1.DeleteOptions{}); !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func (ctrl *VMExportController) handlePodSucceededOrFailed(vmExport *exportv1.VirtualMachineExport, pod *corev1.Pod) error {
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		// The server died or completed, delete the pod.
		return ctrl.deleteExporterPod(vmExport, pod, exporterPodFailedOrCompletedEvent, fmt.Sprintf("Exporter pod %s/%s is in phase %s", pod.Namespace, pod.Name, pod.Status.Phase))
	}
	return nil
}

func (ctrl *VMExportController) isPVCPopulated(pvc *corev1.PersistentVolumeClaim) (bool, error) {
	return cdiv1.IsPopulated(pvc, func(name, namespace string) (*cdiv1.DataVolume, error) {
		obj, exists, err := ctrl.DataVolumeInformer.GetStore().GetByKey(controller.NamespacedKey(namespace, name))
		if err != nil {
			return nil, err
		}
		if exists {
			dv, ok := obj.(*cdiv1.DataVolume)
			if ok {
				return dv, nil
			}
		}
		return nil, fmt.Errorf("datavolume %s/%s not found", namespace, name)
	})
}

func (ctrl *VMExportController) getOrCreateCertSecret(vmExport *exportv1.VirtualMachineExport, ownerPod *corev1.Pod) error {
	_, err := ctrl.Client.CoreV1().Secrets(vmExport.Namespace).Create(context.Background(), ctrl.createCertSecretManifest(vmExport, ownerPod), metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	} else {
		log.Log.V(3).Infof("Created new exporter pod secret")
		ctrl.Recorder.Eventf(vmExport, corev1.EventTypeNormal, secretCreatedEvent, "Created exporter pod secret")
	}
	return nil
}

func (ctrl *VMExportController) createCertSecretManifest(vmExport *exportv1.VirtualMachineExport, ownerPod *corev1.Pod) *corev1.Secret {
	caCert := ctrl.caCertManager.Current()
	caKeyPair := &triple.KeyPair{
		Key:  caCert.PrivateKey.(*rsa.PrivateKey),
		Cert: caCert.Leaf,
	}
	keyPair, _ := triple.NewServerKeyPair(
		caKeyPair,
		fmt.Sprintf(components.LocalPodDNStemplateString, ctrl.getExportServiceName(vmExport), vmExport.Namespace),
		ctrl.getExportServiceName(vmExport),
		vmExport.Namespace,
		components.CaClusterLocal,
		nil,
		nil,
		certExpiry,
	)

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ctrl.getExportSecretName(ownerPod),
			Namespace: vmExport.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(ownerPod, schema.GroupVersionKind{
					Group:   corev1.SchemeGroupVersion.Group,
					Version: corev1.SchemeGroupVersion.Version,
					Kind:    "Pod",
				}),
			},
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			"tls.crt": cert.EncodeCertPEM(keyPair.Cert),
			"tls.key": cert.EncodePrivateKeyPEM(keyPair.Key),
		},
	}
}

func (ctrl *VMExportController) getExportSecretName(ownerPod *corev1.Pod) string {
	var certSecretName string
	for _, volume := range ownerPod.Spec.Volumes {
		if volume.Name == certificates {
			certSecretName = volume.Secret.SecretName
		}
	}
	return certSecretName
}

func (ctrl *VMExportController) getExportServiceName(vmExport *exportv1.VirtualMachineExport) string {
	return naming.GetName(exportPrefix, vmExport.Name, validation.DNS1035LabelMaxLength)
}

func (ctrl *VMExportController) getExportPodName(vmExport *exportv1.VirtualMachineExport) string {
	return naming.GetName(exportPrefix, vmExport.Name, validation.DNS1035LabelMaxLength)
}

func (ctrl *VMExportController) getOrCreateExportService(vmExport *exportv1.VirtualMachineExport) (*corev1.Service, error) {
	key := controller.NamespacedKey(vmExport.Namespace, ctrl.getExportServiceName(vmExport))
	if service, exists, err := ctrl.ServiceInformer.GetStore().GetByKey(key); err != nil {
		return nil, err
	} else if !exists {
		service := ctrl.createServiceManifest(vmExport)
		log.Log.V(3).Infof("Creating new exporter service %s/%s", service.Namespace, service.Name)
		service, err := ctrl.Client.CoreV1().Services(vmExport.Namespace).Create(context.Background(), service, metav1.CreateOptions{})
		if err == nil {
			ctrl.Recorder.Eventf(vmExport, corev1.EventTypeNormal, serviceCreatedEvent, "Created service %s/%s", service.Namespace, service.Name)
		}
		return service, err
	} else {
		return service.(*corev1.Service), nil
	}
}

func (ctrl *VMExportController) createServiceManifest(vmExport *exportv1.VirtualMachineExport) *corev1.Service {
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ctrl.getExportServiceName(vmExport),
			Namespace: vmExport.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(vmExport, schema.GroupVersionKind{
					Group:   exportGVK.Group,
					Version: exportGVK.Version,
					Kind:    exportGVK.Kind,
				}),
			},
			Labels: map[string]string{
				virtv1.AppLabel: exportv1.App,
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Protocol: "TCP",
					Port:     443,
					TargetPort: intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: 8443,
					},
				},
			},
			Selector: map[string]string{
				exportServiceLabel: vmExport.Name,
			},
		},
	}
	return service
}

func (ctrl *VMExportController) getExporterPod(vmExport *exportv1.VirtualMachineExport) (*corev1.Pod, bool, error) {
	key := controller.NamespacedKey(vmExport.Namespace, ctrl.getExportPodName(vmExport))
	if obj, exists, err := ctrl.PodInformer.GetStore().GetByKey(key); err != nil {
		log.Log.V(3).Errorf("error %v", err)
		return nil, false, err
	} else if !exists {
		return nil, exists, nil
	} else {
		pod := obj.(*corev1.Pod)
		return pod, exists, nil
	}
}

func (ctrl *VMExportController) createExporterPod(vmExport *exportv1.VirtualMachineExport, pvcs []*corev1.PersistentVolumeClaim) (*corev1.Pod, error) {
	log.Log.V(3).Infof("Checking if pod exist: %s/%s", vmExport.Namespace, ctrl.getExportPodName(vmExport))
	key := controller.NamespacedKey(vmExport.Namespace, ctrl.getExportPodName(vmExport))
	if obj, exists, err := ctrl.PodInformer.GetStore().GetByKey(key); err != nil {
		log.Log.V(3).Errorf("error %v", err)
		return nil, err
	} else if !exists {
		manifest := ctrl.createExporterPodManifest(vmExport, pvcs)

		log.Log.V(3).Infof("Creating new exporter pod %s/%s", manifest.Namespace, manifest.Name)
		pod, err := ctrl.Client.CoreV1().Pods(vmExport.Namespace).Create(context.Background(), manifest, metav1.CreateOptions{})
		if err == nil {
			ctrl.Recorder.Eventf(vmExport, corev1.EventTypeNormal, exporterPodCreatedEvent, "Created exporter pod %s/%s", manifest.Namespace, manifest.Name)
		}
		return pod, nil
	} else {
		pod := obj.(*corev1.Pod)
		return pod, nil
	}
}

func (ctrl *VMExportController) createExporterPodManifest(vmExport *exportv1.VirtualMachineExport, pvcs []*corev1.PersistentVolumeClaim) *corev1.Pod {
	podManifest := ctrl.TemplateService.RenderExporterManifest(vmExport, exportPrefix)
	podManifest.ObjectMeta.Labels = map[string]string{exportServiceLabel: vmExport.Name}
	podManifest.Spec.SecurityContext = &corev1.PodSecurityContext{
		RunAsNonRoot: pointer.Bool(true),
		RunAsGroup:   pointer.Int64Ptr(kvm),
		FSGroup:      pointer.Int64Ptr(kvm),
	}
	for i, pvc := range pvcs {
		var mountPoint string
		if types.IsPVCBlock(pvc.Spec.VolumeMode) {
			mountPoint = fmt.Sprintf("%s/%s", blockVolumeMountPath, pvc.Name)
			podManifest.Spec.Containers[0].VolumeDevices = append(podManifest.Spec.Containers[0].VolumeDevices, corev1.VolumeDevice{
				Name:       pvc.Name,
				DevicePath: mountPoint,
			})
		} else {
			mountPoint = fmt.Sprintf("%s/%s", fileSystemMountPath, pvc.Name)
			podManifest.Spec.Containers[0].VolumeMounts = append(podManifest.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
				Name:      pvc.Name,
				ReadOnly:  true,
				MountPath: mountPoint,
			})
		}
		podManifest.Spec.Volumes = append(podManifest.Spec.Volumes, corev1.Volume{
			Name: pvc.Name,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvc.Name,
				},
			},
		})
		ctrl.addVolumeEnvironmentVariables(&podManifest.Spec.Containers[0], pvc, i, mountPoint)
	}

	// Add token and certs ENV variables
	podManifest.Spec.Containers[0].Env = append(podManifest.Spec.Containers[0].Env, corev1.EnvVar{
		Name:  "CERT_FILE",
		Value: "/cert/tls.crt",
	}, corev1.EnvVar{
		Name:  "KEY_FILE",
		Value: "/cert/tls.key",
	}, corev1.EnvVar{
		Name:  "TOKEN_FILE",
		Value: "/token/token",
	}, corev1.EnvVar{
		Name:  "DEADLINE",
		Value: currentTime().Add(deadline).Format(time.RFC3339),
	})

	secretName := fmt.Sprintf("secret-%s", rand.String(10))
	podManifest.Spec.Volumes = append(podManifest.Spec.Volumes, corev1.Volume{
		Name: certificates,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: secretName,
			},
		},
	}, corev1.Volume{
		Name: vmExport.Spec.TokenSecretRef,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: vmExport.Spec.TokenSecretRef,
			},
		},
	})

	podManifest.Spec.Containers[0].VolumeMounts = append(podManifest.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      certificates,
		MountPath: "/cert",
	}, corev1.VolumeMount{
		Name:      vmExport.Spec.TokenSecretRef,
		MountPath: "/token",
	})
	return podManifest
}

func (ctrl *VMExportController) addVolumeEnvironmentVariables(exportContainer *corev1.Container, pvc *corev1.PersistentVolumeClaim, index int, mountPoint string) {
	exportContainer.Env = append(exportContainer.Env, corev1.EnvVar{
		Name:  fmt.Sprintf("VOLUME%d_EXPORT_PATH", index),
		Value: mountPoint,
	})
	if types.IsPVCBlock(pvc.Spec.VolumeMode) {
		exportContainer.Env = append(exportContainer.Env, corev1.EnvVar{
			Name:  fmt.Sprintf("VOLUME%d_EXPORT_RAW_URI", index),
			Value: rawURI(pvc),
		}, corev1.EnvVar{
			Name:  fmt.Sprintf("VOLUME%d_EXPORT_RAW_GZIP_URI", index),
			Value: rawGzipURI(pvc),
		})
	} else {
		if ctrl.isKubevirtContentType(pvc) {
			exportContainer.Env = append(exportContainer.Env, corev1.EnvVar{
				Name:  fmt.Sprintf("VOLUME%d_EXPORT_RAW_URI", index),
				Value: rawURI(pvc),
			}, corev1.EnvVar{
				Name:  fmt.Sprintf("VOLUME%d_EXPORT_RAW_GZIP_URI", index),
				Value: rawGzipURI(pvc),
			})
		} else {
			exportContainer.Env = append(exportContainer.Env, corev1.EnvVar{
				Name:  fmt.Sprintf("VOLUME%d_EXPORT_ARCHIVE_URI", index),
				Value: archiveURI(pvc),
			}, corev1.EnvVar{
				Name:  fmt.Sprintf("VOLUME%d_EXPORT_DIR_URI", index),
				Value: dirURI(pvc),
			})
		}
	}
}

func (ctrl *VMExportController) isKubevirtContentType(pvc *corev1.PersistentVolumeClaim) bool {
	// Block volumes are assumed always KubevirtContentType
	if types.IsPVCBlock(pvc.Spec.VolumeMode) {
		return true
	}
	contentType, ok := pvc.Annotations[annContentType]
	isKubevirt := ok && (contentType == string(cdiv1.DataVolumeKubeVirt) || contentType == "")
	if isKubevirt {
		return true
	}
	ownerRef := metav1.GetControllerOf(pvc)
	if ownerRef == nil {
		return false
	}
	if ownerRef.Kind == datavolumeGVK.Kind && ownerRef.APIVersion == datavolumeGVK.GroupVersion().String() {
		obj, exists, err := ctrl.DataVolumeInformer.GetStore().GetByKey(controller.NamespacedKey(pvc.GetNamespace(), ownerRef.Name))
		if err != nil {
			log.Log.V(3).Infof("Error getting DataVolume %v", err)
		}
		if exists {
			dv, ok := obj.(*cdiv1.DataVolume)
			isKubevirt = ok && (dv.Spec.ContentType == cdiv1.DataVolumeKubeVirt || dv.Spec.ContentType == "")
		}
	}
	return isKubevirt
}

func (ctrl *VMExportController) updateCommonVMExportStatusFields(vmExport, vmExportCopy *exportv1.VirtualMachineExport, exporterPod *corev1.Pod, service *corev1.Service, sourceVolumes *sourceVolumes) error {
	var err error
	if vmExportCopy.Status == nil {
		vmExportCopy.Status = &exportv1.VirtualMachineExportStatus{
			Phase: exportv1.Pending,
			Conditions: []exportv1.Condition{
				newReadyCondition(corev1.ConditionFalse, initializingReason, ""),
				newPvcCondition(corev1.ConditionFalse, unknownReason, ""),
			},
		}
	}

	vmExportCopy.Status.ServiceName = service.Name
	vmExportCopy.Status.Links = &exportv1.VirtualMachineExportLinks{}
	if exporterPod == nil {
		vmExportCopy.Status.Conditions = updateCondition(vmExportCopy.Status.Conditions, newReadyCondition(corev1.ConditionFalse, inUseReason, sourceVolumes.availableMessage))
		vmExportCopy.Status.Phase = exportv1.Pending
	} else {
		if exporterPod.Status.Phase == corev1.PodRunning {
			vmExportCopy.Status.Conditions = updateCondition(vmExportCopy.Status.Conditions, newReadyCondition(corev1.ConditionTrue, podReadyReason, ""))
			vmExportCopy.Status.Phase = exportv1.Ready
			vmExportCopy.Status.Links.Internal, err = ctrl.getInteralLinks(sourceVolumes.volumes, exporterPod, service)
			if err != nil {
				return err
			}
			vmExportCopy.Status.Links.External, err = ctrl.getExternalLinks(sourceVolumes.volumes, exporterPod, vmExport)
			if err != nil {
				return err
			}
		} else if exporterPod.Status.Phase == corev1.PodSucceeded {
			vmExportCopy.Status.Conditions = updateCondition(vmExportCopy.Status.Conditions, newReadyCondition(corev1.ConditionFalse, podCompletedReason, ""))
			vmExportCopy.Status.Phase = exportv1.Terminated
		} else if exporterPod.Status.Phase == corev1.PodPending {
			vmExportCopy.Status.Conditions = updateCondition(vmExportCopy.Status.Conditions, newReadyCondition(corev1.ConditionFalse, podPendingReason, ""))
			vmExportCopy.Status.Phase = exportv1.Pending
		} else {
			vmExportCopy.Status.Conditions = updateCondition(vmExportCopy.Status.Conditions, newReadyCondition(corev1.ConditionFalse, unknownReason, ""))
			vmExportCopy.Status.Phase = exportv1.Pending
		}
	}

	return nil
}

func (ctrl *VMExportController) updateVMExportStatus(vmExport, vmExportCopy *exportv1.VirtualMachineExport) error {
	if !equality.Semantic.DeepEqual(vmExport, vmExportCopy) {
		if _, err := ctrl.Client.VirtualMachineExport(vmExportCopy.Namespace).Update(context.Background(), vmExportCopy, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func newReadyCondition(status corev1.ConditionStatus, reason, message string) exportv1.Condition {
	return exportv1.Condition{
		Type:               exportv1.ConditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: *currentTime(),
	}
}

func newPvcCondition(status corev1.ConditionStatus, reason, message string) exportv1.Condition {
	return exportv1.Condition{
		Type:               exportv1.ConditionPVC,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: *currentTime(),
	}
}

func newVolumesCreatedCondition(status corev1.ConditionStatus, reason, message string) exportv1.Condition {
	return exportv1.Condition{
		Type:               exportv1.ConditionVolumesCreated,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: *currentTime(),
	}
}

func updateCondition(conditions []exportv1.Condition, c exportv1.Condition) []exportv1.Condition {
	found := false
	for i := range conditions {
		if conditions[i].Type == c.Type {
			if conditions[i].Status != c.Status || conditions[i].Reason != c.Reason || conditions[i].Message != c.Message {
				conditions[i] = c
			}
			found = true
			break
		}
	}

	if !found {
		conditions = append(conditions, c)
	}

	return conditions
}

func (ctrl *VMExportController) pvcConditionFromPVC(pvcs []*corev1.PersistentVolumeClaim) exportv1.Condition {
	cond := exportv1.Condition{
		Type:               exportv1.ConditionPVC,
		LastTransitionTime: *currentTime(),
	}
	phase := corev1.ClaimBound
	// Figure out most severe status.
	// Bound least, pending more, lost is most severe status
	for _, pvc := range pvcs {
		if pvc.Status.Phase == corev1.ClaimPending && phase != corev1.ClaimLost {
			phase = corev1.ClaimPending
		}
		if pvc.Status.Phase == corev1.ClaimLost {
			phase = corev1.ClaimLost
		}
	}
	switch phase {
	case corev1.ClaimBound:
		cond.Status = corev1.ConditionTrue
		cond.Reason = pvcBoundReason
	case corev1.ClaimPending:
		cond.Status = corev1.ConditionFalse
		cond.Reason = pvcPendingReason
	default:
		cond.Status = corev1.ConditionFalse
		cond.Reason = unknownReason
	}
	return cond
}
