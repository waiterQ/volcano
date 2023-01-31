package podgroup

import (
	"context"
	"strings"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/restmapper"
	"k8s.io/klog"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

func (pg *pgcontroller) deletePGbyPodOwner(pod *v1.Pod) error {
	pgName := pod.Annotations[scheduling.KubeGroupNameAnnotationKey]
	pgItem, err := pg.pgLister.PodGroups(pod.Namespace).Get(pgName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		klog.Errorf("Failed to get normal PodGroup for Pod <%s/%s>: %v", pod.Namespace, pod.Name, err)
		return err
	}

	var owner *metav1.OwnerReference
	for _, item := range pod.OwnerReferences {
		if item.Controller == nil || !*item.Controller {
			continue
		}
		owner = &item
		break
	}
	if owner == nil {
		return nil
	}

	apiversionList := strings.Split(owner.APIVersion, "/")
	var group, version string
	if len(apiversionList) == 2 {
		group, version = apiversionList[0], apiversionList[1]
	} else if len(apiversionList) == 1 {
		version = apiversionList[0]
	}
	// vcjob's podGroup delete by jobController
	if group == "batch.volcano.sh" && owner.Kind == "Job" {
		return nil
	}

	gk := schema.GroupKind{
		Group: group,
		Kind:  owner.Kind,
	}
	unstructuredObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
	if err != nil {
		klog.Errorf("failed to convert pod to unstructed.error=%v", err)
		return nil
	}
	object := &unstructured.Unstructured{unstructuredObj}
	groupResources, err := restmapper.GetAPIGroupResources(pg.kubeClient.Discovery())
	if err != nil {
		klog.Errorf("failed to get resouces(%s:%s)in ns(%s),error=%v", gk.String(), owner.Name, object.GetNamespace(), err)
		return err
	}
	rm := restmapper.NewDiscoveryRESTMapper(groupResources)
	mapping, err := rm.RESTMapping(gk, version)
	parentObj, err := pg.dynamicClient.Resource(mapping.Resource).
		Namespace(object.GetNamespace()).
		Get(context.Background(), owner.Name, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("failed to get resouces(%s:%s)in ns(%s),error=%v", gk.String(), owner.Name, object.GetNamespace(), err)
		return err
	}

	replicas, ok, err := unstructured.NestedInt64(parentObj.Object, "spec", "replicas")
	if err != nil {
		klog.Errorf("failed to convert replicas: %v", err)
		return nil
	}
	// pod owner spec.replicas==0, or no filed replicas will delete
	if ok && replicas != 0 {
		return nil
	}

	if err := pg.vcClient.SchedulingV1beta1().PodGroups(pgItem.Namespace).Delete(context.TODO(), pgName, metav1.DeleteOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("Failed to delete PodGroup %s/%s: %v",
				pgItem.Namespace, pgItem.Name, err)
			return err
		}
	}
	klog.V(3).Infof("success delete pg %s", pgName)
	return nil
}

func (pg *pgcontroller) deletePGbyOwnerAtStartup(item *scheduling.PodGroup) error {
	var owner *metav1.OwnerReference
	for _, item := range item.OwnerReferences {
		if item.Controller == nil || !*item.Controller {
			continue
		}
		owner = &item
		break
	}
	if owner == nil {
		return nil
	}

	apiversionList := strings.Split(owner.APIVersion, "/")
	var group, version string
	if len(apiversionList) == 2 {
		group, version = apiversionList[0], apiversionList[1]
	} else if len(apiversionList) == 1 {
		version = apiversionList[0]
	}
	// vcjob's podGroup delete by jobController
	if group == "batch.volcano.sh" && owner.Kind == "Job" {
		return nil
	}

	gk := schema.GroupKind{
		Group: group,
		Kind:  owner.Kind,
	}
	unstructuredPg, err := runtime.DefaultUnstructuredConverter.ToUnstructured(item)
	if err != nil {
		klog.Errorf("failed to convert pod to unstructed.error=%v", err)
		return nil
	}
	object := &unstructured.Unstructured{unstructuredPg}
	groupResources, err := restmapper.GetAPIGroupResources(pg.kubeClient.Discovery())
	if err != nil {
		klog.Errorf("failed to get resouces(%s:%s)in ns(%s),error=%v", gk.String(), owner.Name, object.GetNamespace(), err)
		return err
	}
	rm := restmapper.NewDiscoveryRESTMapper(groupResources)
	mapping, err := rm.RESTMapping(gk, version)
	parentObj, err := pg.dynamicClient.Resource(mapping.Resource).
		Namespace(object.GetNamespace()).
		Get(context.Background(), owner.Name, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("failed to get resouces(%s:%s)in ns(%s),error=%v", gk.String(), owner.Name, object.GetNamespace(), err)
		return err
	}

	replicas, ok, err := unstructured.NestedInt64(parentObj.Object, "spec", "replicas")
	if err != nil {
		klog.Errorf("failed to convert replicas: %v", err)
		return nil
	}
	// for stock pg, only owner spec.replicas==0 will delete
	if !ok || replicas != 0 {
		return nil
	}

	if err := pg.vcClient.SchedulingV1beta1().PodGroups(item.Namespace).Delete(context.TODO(), item.Name, metav1.DeleteOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("Failed to delete PodGroup %v/%v: %v",
				item.Namespace, item.Name, err)
			return err
		}
	}
	klog.V(3).Infof("success delete pg %s first", item.Name)
	return nil
}
