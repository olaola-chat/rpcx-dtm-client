/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmgrpc

import (
	context "context"
	"github.com/dtm-labs/client/dtmcli/dtmimp"
	"github.com/dtm-labs/client/dtmxrpc/dtmrimp"
	"github.com/dtm-labs/dtmdriver"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func FromDtmError(r interface{}) error {
	return r.(error)
}

// MustGenGid must gen a gid from grpcServer
func MustGenGid(grpcServer string) string {
	dc := dtmrimp.MustGetDtmRpcXClient(grpcServer)
	r, err := dc.NewGid(context.Background(), &emptypb.Empty{})
	dtmimp.E2P(err)
	return r.Gid
}

// UseDriver use the specified driver to handle grpc urls
func UseDriver(driverName string) error {
	return dtmdriver.Use(driverName)
}
