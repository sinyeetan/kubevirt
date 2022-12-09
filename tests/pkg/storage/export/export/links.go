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
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"path"
	"strings"
	"unicode"

	routev1 "github.com/openshift/api/route/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"

	exportv1 "kubevirt.io/api/export/v1alpha1"

	"kubevirt.io/kubevirt/pkg/certificates/triple/cert"
	"kubevirt.io/kubevirt/pkg/controller"
	"kubevirt.io/kubevirt/pkg/virt-operator/resource/generate/components"
)

const (
	caBundle             = "ca-bundle"
	routeCAConfigMapName = "kube-root-ca.crt"
	routeCaKey           = "ca.crt"
	subjectAltNameId     = "2.5.29.17"

	apiGroup              = "export.kubevirt.io"
	apiVersion            = "v1alpha1"
	exportResourceName    = "virtualmachineexports"
	gv                    = apiGroup + "/" + apiVersion
	externalUrlLinkFormat = "/api/" + gv + "/namespaces/%s/" + exportResourceName + "/%s"
)

func (ctrl *VMExportController) getInteralLinks(pvcs []*corev1.PersistentVolumeClaim, exporterPod *corev1.Pod, service *corev1.Service) (*exportv1.VirtualMachineExportLink, error) {
	internalCert, err := ctrl.internalExportCa()
	if err != nil {
		return nil, err
	}
	host := fmt.Sprintf("%s.%s.svc", service.Name, service.Namespace)
	return ctrl.getLinks(pvcs, exporterPod, host, internalCert)
}

func (ctrl *VMExportController) getExternalLinks(pvcs []*corev1.PersistentVolumeClaim, exporterPod *corev1.Pod, export *exportv1.VirtualMachineExport) (*exportv1.VirtualMachineExportLink, error) {
	urlPath := fmt.Sprintf(externalUrlLinkFormat, export.Namespace, export.Name)
	externalLinkHost, cert := ctrl.getExternalLinkHostAndCert()
	if externalLinkHost != "" {
		hostAndBase := path.Join(externalLinkHost, urlPath)
		return ctrl.getLinks(pvcs, exporterPod, hostAndBase, cert)
	}
	return nil, nil
}

func (ctrl *VMExportController) getLinks(pvcs []*corev1.PersistentVolumeClaim, exporterPod *corev1.Pod, hostAndBase, cert string) (*exportv1.VirtualMachineExportLink, error) {
	exportLink := &exportv1.VirtualMachineExportLink{
		Volumes: []exportv1.VirtualMachineExportVolume{},
		Cert:    cert,
	}
	for _, pvc := range pvcs {
		if pvc != nil && exporterPod != nil && exporterPod.Status.Phase == corev1.PodRunning {
			const scheme = "https://"

			if ctrl.isKubevirtContentType(pvc) {
				exportLink.Volumes = append(exportLink.Volumes, exportv1.VirtualMachineExportVolume{
					Name: pvc.Name,
					Formats: []exportv1.VirtualMachineExportVolumeFormat{
						{
							Format: exportv1.KubeVirtRaw,
							Url:    scheme + path.Join(hostAndBase, rawURI(pvc)),
						},
						{
							Format: exportv1.KubeVirtGz,
							Url:    scheme + path.Join(hostAndBase, rawGzipURI(pvc)),
						},
					},
				})
			} else {
				exportLink.Volumes = append(exportLink.Volumes, exportv1.VirtualMachineExportVolume{
					Name: pvc.Name,
					Formats: []exportv1.VirtualMachineExportVolumeFormat{
						{
							Format: exportv1.Dir,
							Url:    scheme + path.Join(hostAndBase, dirURI(pvc)),
						},
						{
							Format: exportv1.ArchiveGz,
							Url:    scheme + path.Join(hostAndBase, archiveURI(pvc)),
						},
					},
				})
			}
		}
	}
	return exportLink, nil
}

func (ctrl *VMExportController) internalExportCa() (string, error) {
	key := controller.NamespacedKey(ctrl.KubevirtNamespace, components.KubeVirtExportCASecretName)
	obj, exists, err := ctrl.ConfigMapInformer.GetStore().GetByKey(key)
	if err != nil || !exists {
		return "", err
	}
	cm := obj.(*corev1.ConfigMap).DeepCopy()
	bundle := cm.Data[caBundle]
	return strings.TrimSpace(bundle), nil
}

func (ctrl *VMExportController) getExternalLinkHostAndCert() (string, string) {
	for _, obj := range ctrl.IngressCache.List() {
		if ingress, ok := obj.(*networkingv1.Ingress); ok {
			if host := getHostFromIngress(ingress); host != "" {
				cert, _ := ctrl.getIngressCert(host, ingress)
				return host, cert
			}
		}
	}
	for _, obj := range ctrl.RouteCache.List() {
		if route, ok := obj.(*routev1.Route); ok {
			if host := getHostFromRoute(route); host != "" {
				cert, _ := ctrl.getRouteCert(host)
				return host, cert
			}
		}
	}
	return "", ""
}

func (ctrl *VMExportController) getIngressCert(hostName string, ing *networkingv1.Ingress) (string, error) {
	secretName := ""
	for _, tls := range ing.Spec.TLS {
		if tls.SecretName != "" {
			secretName = tls.SecretName
			break
		}
	}
	key := controller.NamespacedKey(ctrl.KubevirtNamespace, secretName)
	obj, exists, err := ctrl.SecretInformer.GetStore().GetByKey(key)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", nil
	}
	if secret, ok := obj.(*corev1.Secret); ok {
		return ctrl.getIngressCertFromSecret(secret, hostName)
	}
	return "", nil
}

func (ctrl *VMExportController) getIngressCertFromSecret(secret *corev1.Secret, hostName string) (string, error) {
	certBytes := secret.Data["tls.crt"]
	certs, err := cert.ParseCertsPEM(certBytes)
	if err != nil {
		return "", err
	}
	return ctrl.findCertByHostName(hostName, certs)
}

func (ctrl *VMExportController) getRouteCert(hostName string) (string, error) {
	key := controller.NamespacedKey(ctrl.KubevirtNamespace, routeCAConfigMapName)
	obj, exists, err := ctrl.RouteConfigMapInformer.GetStore().GetByKey(key)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", nil
	}
	if cm, ok := obj.(*corev1.ConfigMap); ok {
		cmString := cm.Data[routeCaKey]
		certs, err := cert.ParseCertsPEM([]byte(cmString))
		if err != nil {
			return "", err
		}
		return ctrl.findCertByHostName(hostName, certs)
	}
	return "", fmt.Errorf("not a config map")
}

func (ctrl *VMExportController) findCertByHostName(hostName string, certs []*x509.Certificate) (string, error) {
	for _, cert := range certs {
		if ctrl.matchesOrWildCard(hostName, cert.Subject.CommonName) {
			return buildPemFromCert(cert)
		}
		for _, extension := range cert.Extensions {
			if extension.Id.String() == subjectAltNameId {
				value := strings.Map(func(r rune) rune {
					if unicode.IsPrint(r) && r <= unicode.MaxASCII {
						return r
					}
					return ' '
				}, string(extension.Value))
				names := strings.Split(value, " ")
				for _, name := range names {
					if ctrl.matchesOrWildCard(hostName, name) {
						return buildPemFromCert(cert)
					}
				}
			}
		}
	}
	return "", nil
}

func buildPemFromCert(cert *x509.Certificate) (string, error) {
	pemOut := strings.Builder{}
	if err := pem.Encode(&pemOut, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}); err != nil {
		return "", err
	}
	return strings.TrimSpace(pemOut.String()), nil
}

func (ctrl *VMExportController) matchesOrWildCard(hostName, compare string) bool {
	wildCard := fmt.Sprintf("*.%s", getDomainFromHost(hostName))
	return hostName == compare || wildCard == compare
}

func getDomainFromHost(host string) string {
	if index := strings.Index(host, "."); index != -1 {
		return host[index+1:]
	}
	return host
}

func getHostFromRoute(route *routev1.Route) string {
	if route.Spec.To.Name == components.VirtExportProxyServiceName {
		if len(route.Status.Ingress) > 0 {
			return route.Status.Ingress[0].Host
		}
	}
	return ""
}

func getHostFromIngress(ing *networkingv1.Ingress) string {
	if ing.Spec.DefaultBackend != nil && ing.Spec.DefaultBackend.Service != nil {
		if ing.Spec.DefaultBackend.Service.Name != components.VirtExportProxyServiceName {
			return ""
		}
		return ing.Spec.Rules[0].Host
	}
	for _, rule := range ing.Spec.Rules {
		if rule.HTTP == nil {
			continue
		}
		for _, path := range rule.HTTP.Paths {
			if path.Backend.Service != nil && path.Backend.Service.Name == components.VirtExportProxyServiceName {
				if rule.Host != "" {
					return rule.Host
				}
			}
		}
	}
	return ""
}
