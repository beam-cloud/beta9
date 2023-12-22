// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v4.25.1
// source: function.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type FunctionInvokeRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ObjectId      string `protobuf:"bytes,1,opt,name=object_id,json=objectId,proto3" json:"object_id,omitempty"`
	ImageId       string `protobuf:"bytes,2,opt,name=image_id,json=imageId,proto3" json:"image_id,omitempty"`
	Args          []byte `protobuf:"bytes,3,opt,name=args,proto3" json:"args,omitempty"`
	Handler       string `protobuf:"bytes,4,opt,name=handler,proto3" json:"handler,omitempty"`
	PythonVersion string `protobuf:"bytes,5,opt,name=python_version,json=pythonVersion,proto3" json:"python_version,omitempty"`
	Cpu           int64  `protobuf:"varint,6,opt,name=cpu,proto3" json:"cpu,omitempty"`
	Memory        int64  `protobuf:"varint,7,opt,name=memory,proto3" json:"memory,omitempty"`
	Gpu           string `protobuf:"bytes,8,opt,name=gpu,proto3" json:"gpu,omitempty"`
}

func (x *FunctionInvokeRequest) Reset() {
	*x = FunctionInvokeRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_function_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FunctionInvokeRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FunctionInvokeRequest) ProtoMessage() {}

func (x *FunctionInvokeRequest) ProtoReflect() protoreflect.Message {
	mi := &file_function_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FunctionInvokeRequest.ProtoReflect.Descriptor instead.
func (*FunctionInvokeRequest) Descriptor() ([]byte, []int) {
	return file_function_proto_rawDescGZIP(), []int{0}
}

func (x *FunctionInvokeRequest) GetObjectId() string {
	if x != nil {
		return x.ObjectId
	}
	return ""
}

func (x *FunctionInvokeRequest) GetImageId() string {
	if x != nil {
		return x.ImageId
	}
	return ""
}

func (x *FunctionInvokeRequest) GetArgs() []byte {
	if x != nil {
		return x.Args
	}
	return nil
}

func (x *FunctionInvokeRequest) GetHandler() string {
	if x != nil {
		return x.Handler
	}
	return ""
}

func (x *FunctionInvokeRequest) GetPythonVersion() string {
	if x != nil {
		return x.PythonVersion
	}
	return ""
}

func (x *FunctionInvokeRequest) GetCpu() int64 {
	if x != nil {
		return x.Cpu
	}
	return 0
}

func (x *FunctionInvokeRequest) GetMemory() int64 {
	if x != nil {
		return x.Memory
	}
	return 0
}

func (x *FunctionInvokeRequest) GetGpu() string {
	if x != nil {
		return x.Gpu
	}
	return ""
}

type FunctionInvokeResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Output   string `protobuf:"bytes,1,opt,name=output,proto3" json:"output,omitempty"`
	Done     bool   `protobuf:"varint,2,opt,name=done,proto3" json:"done,omitempty"`
	ExitCode uint32 `protobuf:"varint,3,opt,name=exit_code,json=exitCode,proto3" json:"exit_code,omitempty"`
}

func (x *FunctionInvokeResponse) Reset() {
	*x = FunctionInvokeResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_function_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FunctionInvokeResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FunctionInvokeResponse) ProtoMessage() {}

func (x *FunctionInvokeResponse) ProtoReflect() protoreflect.Message {
	mi := &file_function_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FunctionInvokeResponse.ProtoReflect.Descriptor instead.
func (*FunctionInvokeResponse) Descriptor() ([]byte, []int) {
	return file_function_proto_rawDescGZIP(), []int{1}
}

func (x *FunctionInvokeResponse) GetOutput() string {
	if x != nil {
		return x.Output
	}
	return ""
}

func (x *FunctionInvokeResponse) GetDone() bool {
	if x != nil {
		return x.Done
	}
	return false
}

func (x *FunctionInvokeResponse) GetExitCode() uint32 {
	if x != nil {
		return x.ExitCode
	}
	return 0
}

type FunctionGetArgsRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	InvocationId string `protobuf:"bytes,1,opt,name=invocation_id,json=invocationId,proto3" json:"invocation_id,omitempty"`
}

func (x *FunctionGetArgsRequest) Reset() {
	*x = FunctionGetArgsRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_function_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FunctionGetArgsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FunctionGetArgsRequest) ProtoMessage() {}

func (x *FunctionGetArgsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_function_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FunctionGetArgsRequest.ProtoReflect.Descriptor instead.
func (*FunctionGetArgsRequest) Descriptor() ([]byte, []int) {
	return file_function_proto_rawDescGZIP(), []int{2}
}

func (x *FunctionGetArgsRequest) GetInvocationId() string {
	if x != nil {
		return x.InvocationId
	}
	return ""
}

type FunctionGetArgsResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ok   bool   `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
	Args []byte `protobuf:"bytes,2,opt,name=args,proto3" json:"args,omitempty"`
}

func (x *FunctionGetArgsResponse) Reset() {
	*x = FunctionGetArgsResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_function_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FunctionGetArgsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FunctionGetArgsResponse) ProtoMessage() {}

func (x *FunctionGetArgsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_function_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FunctionGetArgsResponse.ProtoReflect.Descriptor instead.
func (*FunctionGetArgsResponse) Descriptor() ([]byte, []int) {
	return file_function_proto_rawDescGZIP(), []int{3}
}

func (x *FunctionGetArgsResponse) GetOk() bool {
	if x != nil {
		return x.Ok
	}
	return false
}

func (x *FunctionGetArgsResponse) GetArgs() []byte {
	if x != nil {
		return x.Args
	}
	return nil
}

type FunctionSetResultRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	InvocationId string `protobuf:"bytes,1,opt,name=invocation_id,json=invocationId,proto3" json:"invocation_id,omitempty"`
	ExitCode     uint32 `protobuf:"varint,2,opt,name=exit_code,json=exitCode,proto3" json:"exit_code,omitempty"`
	Result       []byte `protobuf:"bytes,3,opt,name=result,proto3" json:"result,omitempty"`
}

func (x *FunctionSetResultRequest) Reset() {
	*x = FunctionSetResultRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_function_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FunctionSetResultRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FunctionSetResultRequest) ProtoMessage() {}

func (x *FunctionSetResultRequest) ProtoReflect() protoreflect.Message {
	mi := &file_function_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FunctionSetResultRequest.ProtoReflect.Descriptor instead.
func (*FunctionSetResultRequest) Descriptor() ([]byte, []int) {
	return file_function_proto_rawDescGZIP(), []int{4}
}

func (x *FunctionSetResultRequest) GetInvocationId() string {
	if x != nil {
		return x.InvocationId
	}
	return ""
}

func (x *FunctionSetResultRequest) GetExitCode() uint32 {
	if x != nil {
		return x.ExitCode
	}
	return 0
}

func (x *FunctionSetResultRequest) GetResult() []byte {
	if x != nil {
		return x.Result
	}
	return nil
}

type FunctionSetResultResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ok   bool   `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
	Args []byte `protobuf:"bytes,2,opt,name=args,proto3" json:"args,omitempty"`
}

func (x *FunctionSetResultResponse) Reset() {
	*x = FunctionSetResultResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_function_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FunctionSetResultResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FunctionSetResultResponse) ProtoMessage() {}

func (x *FunctionSetResultResponse) ProtoReflect() protoreflect.Message {
	mi := &file_function_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FunctionSetResultResponse.ProtoReflect.Descriptor instead.
func (*FunctionSetResultResponse) Descriptor() ([]byte, []int) {
	return file_function_proto_rawDescGZIP(), []int{5}
}

func (x *FunctionSetResultResponse) GetOk() bool {
	if x != nil {
		return x.Ok
	}
	return false
}

func (x *FunctionSetResultResponse) GetArgs() []byte {
	if x != nil {
		return x.Args
	}
	return nil
}

var File_function_proto protoreflect.FileDescriptor

var file_function_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x08, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0xe0, 0x01, 0x0a, 0x15, 0x46,
	0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x6e, 0x76, 0x6f, 0x6b, 0x65, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x1b, 0x0a, 0x09, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x69,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x49,
	0x64, 0x12, 0x19, 0x0a, 0x08, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x07, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x49, 0x64, 0x12, 0x12, 0x0a, 0x04,
	0x61, 0x72, 0x67, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x61, 0x72, 0x67, 0x73,
	0x12, 0x18, 0x0a, 0x07, 0x68, 0x61, 0x6e, 0x64, 0x6c, 0x65, 0x72, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x07, 0x68, 0x61, 0x6e, 0x64, 0x6c, 0x65, 0x72, 0x12, 0x25, 0x0a, 0x0e, 0x70, 0x79,
	0x74, 0x68, 0x6f, 0x6e, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x05, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0d, 0x70, 0x79, 0x74, 0x68, 0x6f, 0x6e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f,
	0x6e, 0x12, 0x10, 0x0a, 0x03, 0x63, 0x70, 0x75, 0x18, 0x06, 0x20, 0x01, 0x28, 0x03, 0x52, 0x03,
	0x63, 0x70, 0x75, 0x12, 0x16, 0x0a, 0x06, 0x6d, 0x65, 0x6d, 0x6f, 0x72, 0x79, 0x18, 0x07, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x06, 0x6d, 0x65, 0x6d, 0x6f, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x67,
	0x70, 0x75, 0x18, 0x08, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x67, 0x70, 0x75, 0x22, 0x61, 0x0a,
	0x16, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x6e, 0x76, 0x6f, 0x6b, 0x65, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x6f, 0x75, 0x74, 0x70, 0x75,
	0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x6f, 0x75, 0x74, 0x70, 0x75, 0x74, 0x12,
	0x12, 0x0a, 0x04, 0x64, 0x6f, 0x6e, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x04, 0x64,
	0x6f, 0x6e, 0x65, 0x12, 0x1b, 0x0a, 0x09, 0x65, 0x78, 0x69, 0x74, 0x5f, 0x63, 0x6f, 0x64, 0x65,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x08, 0x65, 0x78, 0x69, 0x74, 0x43, 0x6f, 0x64, 0x65,
	0x22, 0x3d, 0x0a, 0x16, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x47, 0x65, 0x74, 0x41,
	0x72, 0x67, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x23, 0x0a, 0x0d, 0x69, 0x6e,
	0x76, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0c, 0x69, 0x6e, 0x76, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x22,
	0x3d, 0x0a, 0x17, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x47, 0x65, 0x74, 0x41, 0x72,
	0x67, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x6f, 0x6b,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x02, 0x6f, 0x6b, 0x12, 0x12, 0x0a, 0x04, 0x61, 0x72,
	0x67, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x61, 0x72, 0x67, 0x73, 0x22, 0x74,
	0x0a, 0x18, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x53, 0x65, 0x74, 0x52, 0x65, 0x73,
	0x75, 0x6c, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x23, 0x0a, 0x0d, 0x69, 0x6e,
	0x76, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0c, 0x69, 0x6e, 0x76, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x12,
	0x1b, 0x0a, 0x09, 0x65, 0x78, 0x69, 0x74, 0x5f, 0x63, 0x6f, 0x64, 0x65, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0d, 0x52, 0x08, 0x65, 0x78, 0x69, 0x74, 0x43, 0x6f, 0x64, 0x65, 0x12, 0x16, 0x0a, 0x06,
	0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x06, 0x72, 0x65,
	0x73, 0x75, 0x6c, 0x74, 0x22, 0x3f, 0x0a, 0x19, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x53, 0x65, 0x74, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x12, 0x0e, 0x0a, 0x02, 0x6f, 0x6b, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x02, 0x6f,
	0x6b, 0x12, 0x12, 0x0a, 0x04, 0x61, 0x72, 0x67, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x04, 0x61, 0x72, 0x67, 0x73, 0x32, 0xa4, 0x02, 0x0a, 0x0f, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69,
	0x6f, 0x6e, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x57, 0x0a, 0x0e, 0x46, 0x75, 0x6e,
	0x63, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x6e, 0x76, 0x6f, 0x6b, 0x65, 0x12, 0x1f, 0x2e, 0x66, 0x75,
	0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x49,
	0x6e, 0x76, 0x6f, 0x6b, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x20, 0x2e, 0x66,
	0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x49, 0x6e, 0x76, 0x6f, 0x6b, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00,
	0x30, 0x01, 0x12, 0x58, 0x0a, 0x0f, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x47, 0x65,
	0x74, 0x41, 0x72, 0x67, 0x73, 0x12, 0x20, 0x2e, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x2e, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x47, 0x65, 0x74, 0x41, 0x72, 0x67, 0x73,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x21, 0x2e, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69,
	0x6f, 0x6e, 0x2e, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x47, 0x65, 0x74, 0x41, 0x72,
	0x67, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x5e, 0x0a, 0x11,
	0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x53, 0x65, 0x74, 0x52, 0x65, 0x73, 0x75, 0x6c,
	0x74, 0x12, 0x22, 0x2e, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x46, 0x75, 0x6e,
	0x63, 0x74, 0x69, 0x6f, 0x6e, 0x53, 0x65, 0x74, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x23, 0x2e, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x2e, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x53, 0x65, 0x74, 0x52, 0x65, 0x73, 0x75,
	0x6c, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x22, 0x5a, 0x20,
	0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x62, 0x65, 0x61, 0x6d, 0x2d,
	0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2f, 0x62, 0x65, 0x61, 0x6d, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_function_proto_rawDescOnce sync.Once
	file_function_proto_rawDescData = file_function_proto_rawDesc
)

func file_function_proto_rawDescGZIP() []byte {
	file_function_proto_rawDescOnce.Do(func() {
		file_function_proto_rawDescData = protoimpl.X.CompressGZIP(file_function_proto_rawDescData)
	})
	return file_function_proto_rawDescData
}

var file_function_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_function_proto_goTypes = []interface{}{
	(*FunctionInvokeRequest)(nil),     // 0: function.FunctionInvokeRequest
	(*FunctionInvokeResponse)(nil),    // 1: function.FunctionInvokeResponse
	(*FunctionGetArgsRequest)(nil),    // 2: function.FunctionGetArgsRequest
	(*FunctionGetArgsResponse)(nil),   // 3: function.FunctionGetArgsResponse
	(*FunctionSetResultRequest)(nil),  // 4: function.FunctionSetResultRequest
	(*FunctionSetResultResponse)(nil), // 5: function.FunctionSetResultResponse
}
var file_function_proto_depIdxs = []int32{
	0, // 0: function.FunctionService.FunctionInvoke:input_type -> function.FunctionInvokeRequest
	2, // 1: function.FunctionService.FunctionGetArgs:input_type -> function.FunctionGetArgsRequest
	4, // 2: function.FunctionService.FunctionSetResult:input_type -> function.FunctionSetResultRequest
	1, // 3: function.FunctionService.FunctionInvoke:output_type -> function.FunctionInvokeResponse
	3, // 4: function.FunctionService.FunctionGetArgs:output_type -> function.FunctionGetArgsResponse
	5, // 5: function.FunctionService.FunctionSetResult:output_type -> function.FunctionSetResultResponse
	3, // [3:6] is the sub-list for method output_type
	0, // [0:3] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_function_proto_init() }
func file_function_proto_init() {
	if File_function_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_function_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FunctionInvokeRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_function_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FunctionInvokeResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_function_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FunctionGetArgsRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_function_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FunctionGetArgsResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_function_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FunctionSetResultRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_function_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FunctionSetResultResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_function_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_function_proto_goTypes,
		DependencyIndexes: file_function_proto_depIdxs,
		MessageInfos:      file_function_proto_msgTypes,
	}.Build()
	File_function_proto = out.File
	file_function_proto_rawDesc = nil
	file_function_proto_goTypes = nil
	file_function_proto_depIdxs = nil
}
