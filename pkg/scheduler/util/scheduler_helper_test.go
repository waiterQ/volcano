/*
Copyright 2019 The Kubernetes Authors.

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

package util

import (
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"volcano.sh/volcano/pkg/scheduler/api"
)

func TestSelectBestNode(t *testing.T) {
	cases := []struct {
		NodeScores map[float64][]*api.NodeInfo
		// Expected node is one of ExpectedNodes
		ExpectedNodes []*api.NodeInfo
	}{
		{
			NodeScores: map[float64][]*api.NodeInfo{
				1.0: {&api.NodeInfo{Name: "node1"}, &api.NodeInfo{Name: "node2"}},
				2.0: {&api.NodeInfo{Name: "node3"}, &api.NodeInfo{Name: "node4"}},
			},
			ExpectedNodes: []*api.NodeInfo{{Name: "node3"}, {Name: "node4"}},
		},
		{
			NodeScores: map[float64][]*api.NodeInfo{
				1.0: {&api.NodeInfo{Name: "node1"}, &api.NodeInfo{Name: "node2"}},
				3.0: {&api.NodeInfo{Name: "node3"}},
				2.0: {&api.NodeInfo{Name: "node4"}, &api.NodeInfo{Name: "node5"}},
			},
			ExpectedNodes: []*api.NodeInfo{{Name: "node3"}},
		},
		{
			NodeScores:    map[float64][]*api.NodeInfo{},
			ExpectedNodes: []*api.NodeInfo{nil},
		},
	}

	oneOf := func(node *api.NodeInfo, nodes []*api.NodeInfo) bool {
		for _, v := range nodes {
			if reflect.DeepEqual(node, v) {
				return true
			}
		}
		return false
	}
	for i, test := range cases {
		result := SelectBestNode(test.NodeScores)
		if !oneOf(result, test.ExpectedNodes) {
			t.Errorf("Failed test case #%d, expected: %#v, got %#v", i, test.ExpectedNodes, result)
		}
	}
}

func TestGetMinInt(t *testing.T) {
	cases := []struct {
		vals   []int
		result int
	}{
		{
			vals:   []int{1, 2, 3},
			result: 1,
		},
		{
			vals:   []int{10, 9, 8},
			result: 8,
		},
		{
			vals:   []int{10, 0, 8},
			result: 0,
		},
		{
			vals:   []int{},
			result: 0,
		},
		{
			vals:   []int{0, -1, 1},
			result: -1,
		},
	}
	for i, test := range cases {
		result := GetMinInt(test.vals...)
		if result != test.result {
			t.Errorf("Failed test case #%d, expected: %#v, got %#v", i, test.result, result)
		}
	}
}

func TestConvertRes2ResList(t *testing.T) {
	cases := []struct {
		name string
		Res *api.Resource
		Expect v1.ResourceList
	}{
		{
			name: "mutiResources",
			Res: &api.Resource{
				MilliCPU:        4000,
				Memory:          4000,
				ScalarResources: map[v1.ResourceName]float64{"scalar.test/scalar1": 1, "scalar.test/scalar2": 2},
			},
			Expect: v1.ResourceList{
				v1.ResourceCPU: *resource.NewMilliQuantity(4000, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewQuantity(4000, resource.BinarySI),
				"scalar.test/scalar1": *resource.NewMilliQuantity(1, resource.DecimalSI),
				"scalar.test/scalar2": *resource.NewMilliQuantity(2, resource.DecimalSI),
			},
		},
		{
			name: "null Resources",
			Res: nil,
			Expect: v1.ResourceList{},
		},
	}

	for _, test := range cases {
		result := ConvertRes2ResList(test.Res)
		if !reflect.DeepEqual(result, test.Expect) {
			t.Errorf("Failed test case #%s, expected: %#v, got %#v", test.name, test.Expect, result)
		}
	}
}