// Copyright 2023 The Kubeflow Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package deepspeed

import "sort"

// eventMessageLimit is the maximum size of an Event's message.
// From: k8s.io/kubernetes/pkg/apis/core/validation/events.go
const eventMessageLimit = 1024

func newInt32(v int32) *int32 {
	return &v
}

// truncateMessage truncates a message if it hits the NoteLengthLimit.
func truncateMessage(message string) string {
	if len(message) <= eventMessageLimit {
		return message
	}
	suffix := "..."
	return message[:eventMessageLimit-len(suffix)] + suffix
}

// keysFromData returns sorted keys of a map
func keysFromData(data map[string][]byte) []string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
