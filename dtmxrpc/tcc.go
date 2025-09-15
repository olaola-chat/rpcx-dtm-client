/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmgrpc

import (
	context "context"
	"fmt"
	"github.com/olaola-chat/rpcx-dtm-client/dtmxrpc/dtmgpb"
	"github.com/olaola-chat/rpcx-dtm-client/dtmxrpc/dtmrimp"

	"github.com/olaola-chat/rpcx-dtm-client/dtmcli/dtmimp"
	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// TccRpcX struct of tcc
type TccRpcX struct {
	dtmimp.TransBase
}

// TccGlobalFunc type of global tcc call
type TccGlobalFunc func(tcc *TccRpcX) error

// TccGlobalTransaction begin a tcc global transaction
// dtm dtm server url
// gid global transaction id
// tccFunc tcc busi func, define the transaction logic
func TccGlobalTransaction(dtm string, gid string, tccFunc TccGlobalFunc) (rerr error) {
	return TccGlobalTransaction2(dtm, gid, func(tg *TccRpcX) {}, tccFunc)
}

// TccGlobalTransaction2 new version of TccGlobalTransaction
func TccGlobalTransaction2(dtm string, gid string, custom func(*TccRpcX), tccFunc TccGlobalFunc) (rerr error) {
	tcc := &TccRpcX{TransBase: *dtmimp.NewTransBase(gid, "tcc", dtm, "")}
	custom(tcc)
	rerr = dtmrimp.DtmRpcXCall(&tcc.TransBase, "Prepare")
	if rerr != nil {
		return rerr
	}
	defer dtmimp.DeferDo(&rerr, func() error {
		return dtmrimp.DtmRpcXCall(&tcc.TransBase, "Submit")
	}, func() error {
		tcc.RollbackReason = rerr.Error()
		return dtmrimp.DtmRpcXCall(&tcc.TransBase, "Abort")
	})
	return tccFunc(tcc)
}

// TccFromGrpc tcc from request info
func TccFromGrpc(ctx context.Context) (*TccRpcX, error) {
	tcc := &TccRpcX{
		TransBase: *dtmrimp.TransBaseFromRpcX(ctx),
	}
	if tcc.Dtm == "" || tcc.Gid == "" {
		return nil, fmt.Errorf("bad tcc info. dtm: %s, gid: %s branchid: %s", tcc.Dtm, tcc.Gid, tcc.BranchID)
	}
	return tcc, nil
}

// CallBranch call a tcc branch
func (t *TccRpcX) CallBranch(busiMsg proto.Message, tryURL string, confirmURL string, cancelURL string, reply interface{}, opts ...grpc.CallOption) error {
	branchID := t.NewSubBranchID()
	bd, err := proto.Marshal(busiMsg)
	if err == nil {
		_, err = dtmrimp.MustGetDtmRpcXClient(t.Dtm).RegisterBranch(context.Background(), &dtmgpb.DtmBranchRequest{
			Gid:         t.Gid,
			TransType:   t.TransType,
			BranchID:    branchID,
			BusiPayload: bd,
			Data:        map[string]string{"confirm": confirmURL, "cancel": cancelURL},
		})
	}
	if err != nil {
		return err
	}
	return dtmrimp.InvokeBranch(&t.TransBase, false, busiMsg, tryURL, reply, branchID, "try", opts...)
}
