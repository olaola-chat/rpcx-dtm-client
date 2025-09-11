/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmrimp

import (
	"github.com/dtm-labs/client/dtmcli/dtmimp"
	"github.com/dtm-labs/dtmdriver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// InvokeBranch invoke a url for trans
func InvokeBranch(t *dtmimp.TransBase, isRaw bool, msg proto.Message, url string, reply interface{}, branchID string, op string, opts ...grpc.CallOption) error {
	server, method, err := dtmdriver.GetDriver().ParseServerMethod(url)
	if err != nil {
		return err
	}
	ctx := TransInfo2Ctx(t.Context, t.Gid, t.TransType, branchID, op, t.Dtm)
	ctx = metadata.AppendToOutgoingContext(ctx, Map2Kvs(t.BranchHeaders)...)
	if t.TransType == "xa" { // xa branch need additional phase2_url
		ctx = metadata.AppendToOutgoingContext(ctx, Map2Kvs(map[string]string{dtmpre + "phase2_url": url})...)
	}
	return MustGetRpcXClient(server).Call(ctx, method, msg, reply)
}
