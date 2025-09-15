package workflow

import (
	"context"
	"github.com/olaola-chat/rpcx-dtm-client/dtmcli/dtmimp"
	xrpc "github.com/olaola-chat/rpcx-dtm-client/dtmxrpc"
	"github.com/olaola-chat/rpcx-dtm-client/dtmxrpc/dtmrimp"
	"github.com/olaola-chat/rpcx-dtm-client/workflow/wfpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type workflowServer struct {
	wfpb.UnimplementedWorkflowServer
}

func (s *workflowServer) Execute(ctx context.Context, wd *wfpb.WorkflowData) (*emptypb.Empty, error) {
	if defaultFac.protocol != dtmimp.ProtocolGRPC {
		return nil, status.Errorf(codes.Internal, "workflow server not inited. please call workflow.InitGrpc first")
	}
	tb := dtmrimp.TransBaseFromRpcX(ctx)
	_, err := defaultFac.execute(ctx, tb.Op, tb.Gid, wd.Data)
	return &emptypb.Empty{}, xrpc.FromDtmError(err)
	//return &emptypb.Empty{}, dtmxrpc.DtmError2GrpcError(err)
}
