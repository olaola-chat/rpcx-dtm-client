/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmgrpc

import (
	"github.com/olaola-chat/rpcx-dtm-client/dtmcli"
	"github.com/olaola-chat/rpcx-dtm-client/dtmxrpc/dtmrimp"
	"google.golang.org/protobuf/proto"
)

// SagaRpcX struct of saga
type SagaRpcX struct {
	dtmcli.Saga
}

// NewSagaRpcX create a saga
func NewSagaRpcX(server string, gid string, opts ...TransBaseOption) *SagaRpcX {
	sg := &SagaRpcX{Saga: *dtmcli.NewSaga(server, gid)}

	for _, opt := range opts {
		opt(&sg.TransBase)
	}

	return sg
}

// Add add a saga step
func (s *SagaRpcX) Add(action string, compensate string, payload proto.Message) *SagaRpcX {
	s.Steps = append(s.Steps, map[string]string{"action": action, "compensate": compensate})
	s.BinPayloads = append(s.BinPayloads, dtmrimp.MustProtoMarshal(payload))
	return s
}

// AddBranchOrder specify that branch should be after preBranches. branch should is larger than all the element in preBranches
func (s *SagaRpcX) AddBranchOrder(branch int, preBranches []int) *SagaRpcX {
	s.Saga.AddBranchOrder(branch, preBranches)
	return s
}

// EnableConcurrent enable the concurrent exec of sub trans
func (s *SagaRpcX) EnableConcurrent() *SagaRpcX {
	s.Saga.SetConcurrent()
	return s
}

// Submit submit the saga trans
func (s *SagaRpcX) Submit() error {
	s.Saga.BuildCustomOptions()
	return dtmrimp.DtmRpcXCall(&s.Saga.TransBase, "Submit")
}
