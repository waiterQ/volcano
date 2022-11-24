/*
Copyright 2021 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package jobseq

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1helper "k8s.io/kubernetes/pkg/scheduler/util"

	busv1alpha1 "volcano.sh/apis/pkg/apis/bus/v1alpha1"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/cli/util"
	"volcano.sh/volcano/pkg/scheduler/api"
	e2eutil "volcano.sh/volcano/test/e2e/util"
)

var _ = Describe("Queue E2E Test", func() {
	It("Queue Command Close And Open With State Check", func() {
		q1 := "queue-command-close"
		defaultQueue := "default"
		ctx := e2eutil.InitTestContext(e2eutil.Options{
			Queues: []string{q1},
		})
		defer e2eutil.CleanupTestContext(ctx)

		By("Close queue command check")
		err := util.CreateQueueCommand(ctx.Vcclient, defaultQueue, q1, busv1alpha1.CloseQueueAction)
		if err != nil {
			Expect(err).NotTo(HaveOccurred(), "Error send close queue command")
		}

		err = e2eutil.WaitQueueStatus(func() (bool, error) {
			queue, err := ctx.Vcclient.SchedulingV1beta1().Queues().Get(context.TODO(), q1, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred(), "Get queue %s failed", q1)
			return queue.Status.State == schedulingv1beta1.QueueStateClosed, nil
		})
		Expect(err).NotTo(HaveOccurred(), "Wait for closed queue %s failed", q1)

		By("Open queue command check")
		err = util.CreateQueueCommand(ctx.Vcclient, defaultQueue, q1, busv1alpha1.OpenQueueAction)
		if err != nil {
			Expect(err).NotTo(HaveOccurred(), "Error send open queue command")
		}

		err = e2eutil.WaitQueueStatus(func() (bool, error) {
			queue, err := ctx.Vcclient.SchedulingV1beta1().Queues().Get(context.TODO(), q1, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred(), "Get queue %s failed", q1)
			return queue.Status.State == schedulingv1beta1.QueueStateOpen, nil
		})
		Expect(err).NotTo(HaveOccurred(), "Wait for reopen queue %s failed", q1)

	})

	It("Queue status allocated Check", func() {
		q1 := "q-test-allocated"
		ctx := e2eutil.InitTestContext(e2eutil.Options{
			Queues: []string{q1},
		})
		defer e2eutil.CleanupTestContext(ctx)

		err := e2eutil.WaitQueueStatus(func() (bool, error) {
			queue, err := ctx.Vcclient.SchedulingV1beta1().Queues().Get(context.TODO(), q1, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred(), "Get queue %s failed", q1)
			return queue.Status.State == schedulingv1beta1.QueueStateOpen, nil
		})
		Expect(err).NotTo(HaveOccurred(), "Wait for queue %s open failed", q1)

		job1 := e2eutil.CreateJob(ctx, &e2eutil.JobSpec{
			Name: "job1",
			Queue: q1,
			Tasks: []e2eutil.TaskSpec{
				{
					Img: e2eutil.DefaultBusyBoxImage,
					Req: e2eutil.HalfCPU,
					Command: "sleep 300s && exit 3",
					Rep: 2,
				},
				{
					Img: e2eutil.DefaultBusyBoxImage,
					Req: e2eutil.CPU1Mem1,
					Rep: 1,
					Command: "sleep 300s && exit 3",
				},
			},
		})

		err = e2eutil.WaitJobReady(ctx, job1)
		Expect(err).NotTo(HaveOccurred())

		By("queue allocated check 1")
		expectAllocated := &api.Resource{
			MilliCPU:        2000,
			Memory:          1000,
		}
		err = e2eutil.WaitQueueStatus(func() (bool, error) {
			queue, err := ctx.Vcclient.SchedulingV1beta1().Queues().Get(context.TODO(), q1, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred(), "Get queue %s failed", q1)
			allocated := ConvertResource(queue.Status.Allocated)
			allocated.Memory /= 1<<30 // Convert to million

			return allocated.Equal(expectAllocated, api.Zero), nil
		})
		Expect(err).NotTo(HaveOccurred(), "Wait for queue %s allocated check 1 failed", q1)

		job1.Spec.Tasks[0].Replicas = 1
		job1.Spec.Tasks[0].MinAvailable = &job1.Spec.Tasks[0].Replicas
		job1.Spec.Tasks[1].Replicas = 2
		job1.Spec.Tasks[1].MinAvailable = &job1.Spec.Tasks[1].Replicas
		err = e2eutil.UpdateJob(ctx, job1)
		Expect(err).NotTo(HaveOccurred())

		err = e2eutil.WaitJobReady(ctx, job1)
		Expect(err).NotTo(HaveOccurred())

		By("queue allocated check 2")
		expectAllocated = &api.Resource{
			MilliCPU:        2500,
			Memory:          2000,
		}
		err = e2eutil.WaitQueueStatus(func() (bool, error) {
			queue, err := ctx.Vcclient.SchedulingV1beta1().Queues().Get(context.TODO(), q1, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred(), "Get queue %s failed", q1)
			allocated := ConvertResource(queue.Status.Allocated)
			allocated.Memory /= 1024*1024*1024 // Convert to million

			return allocated.Equal(expectAllocated, api.Zero), nil
		})
		Expect(err).NotTo(HaveOccurred(), "Wait for queue %s allocated check 2 failed", q1)

		job1.Spec.Tasks[0].Replicas = 0
		job1.Spec.Tasks[0].MinAvailable = &job1.Spec.Tasks[0].Replicas
		job1.Spec.Tasks[1].Replicas = 0
		job1.Spec.Tasks[1].MinAvailable = &job1.Spec.Tasks[1].Replicas
		job1.Spec.MinAvailable = 0
		err = e2eutil.UpdateJob(ctx, job1)
		Expect(err).NotTo(HaveOccurred())

		err = e2eutil.WaitJobReady(ctx, job1)
		Expect(err).NotTo(HaveOccurred())

		By("queue allocated check 3")
		expectAllocated = &api.Resource{}
		err = e2eutil.WaitQueueStatus(func() (bool, error) {
			queue, err := ctx.Vcclient.SchedulingV1beta1().Queues().Get(context.TODO(), q1, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred(), "Get queue %s failed", q1)
			allocated := ConvertResource(queue.Status.Allocated)
			allocated.Memory /= 1<<30 // Convert to million

			return allocated.Equal(expectAllocated, api.Zero), nil
		})
		Expect(err).NotTo(HaveOccurred(), "Wait for queue %s allocated check 3 failed", q1)
	})
})

// ConvertResource creates a new resource object from resource list
func ConvertResource(rl v1.ResourceList) *api.Resource {
	r := api.EmptyResource()
	for rName, rQuant := range rl {
		switch rName {
		case v1.ResourceCPU:
			r.MilliCPU += float64(rQuant.MilliValue())
		case v1.ResourceMemory:
			r.Memory += float64(rQuant.MilliValue()) // diff
		case v1.ResourcePods:
			r.MaxTaskNum += int(rQuant.Value())
		case v1.ResourceEphemeralStorage:
			r.AddScalar(rName, float64(rQuant.MilliValue()))
		default:
			if api.IsCountQuota(rName) {
				continue
			}
			//NOTE: When converting this back to k8s resource, we need record the format as well as / 1000
			if v1helper.IsScalarResourceName(rName) {
				r.AddScalar(rName, float64(rQuant.MilliValue()))
			}
		}
	}
	return r
}