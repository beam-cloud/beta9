// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v4.25.1
// source: endpoint.proto

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

type EndpointServeRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StubId string `protobuf:"bytes,1,opt,name=stub_id,json=stubId,proto3" json:"stub_id,omitempty"`
}

func (x *EndpointServeRequest) Reset() {
	*x = EndpointServeRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_endpoint_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EndpointServeRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EndpointServeRequest) ProtoMessage() {}

func (x *EndpointServeRequest) ProtoReflect() protoreflect.Message {
	mi := &file_endpoint_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EndpointServeRequest.ProtoReflect.Descriptor instead.
func (*EndpointServeRequest) Descriptor() ([]byte, []int) {
	return file_endpoint_proto_rawDescGZIP(), []int{0}
}

func (x *EndpointServeRequest) GetStubId() string {
	if x != nil {
		return x.StubId
	}
	return ""
}

type EndpointServeResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ok bool `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
}

func (x *EndpointServeResponse) Reset() {
	*x = EndpointServeResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_endpoint_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EndpointServeResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EndpointServeResponse) ProtoMessage() {}

func (x *EndpointServeResponse) ProtoReflect() protoreflect.Message {
	mi := &file_endpoint_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EndpointServeResponse.ProtoReflect.Descriptor instead.
func (*EndpointServeResponse) Descriptor() ([]byte, []int) {
	return file_endpoint_proto_rawDescGZIP(), []int{1}
}

func (x *EndpointServeResponse) GetOk() bool {
	if x != nil {
		return x.Ok
	}
	return false
}

var File_endpoint_proto protoreflect.FileDescriptor

var file_endpoint_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x65, 0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x08, 0x65, 0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x22, 0x2f, 0x0a, 0x14, 0x45, 0x6e,
	0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x53, 0x65, 0x72, 0x76, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x17, 0x0a, 0x07, 0x73, 0x74, 0x75, 0x62, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x74, 0x75, 0x62, 0x49, 0x64, 0x22, 0x27, 0x0a, 0x15, 0x45,
	0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x53, 0x65, 0x72, 0x76, 0x65, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x6f, 0x6b, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08,
	0x52, 0x02, 0x6f, 0x6b, 0x32, 0x65, 0x0a, 0x0f, 0x45, 0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74,
	0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x52, 0x0a, 0x0d, 0x45, 0x6e, 0x64, 0x70, 0x6f,
	0x69, 0x6e, 0x74, 0x53, 0x65, 0x72, 0x76, 0x65, 0x12, 0x1e, 0x2e, 0x65, 0x6e, 0x64, 0x70, 0x6f,
	0x69, 0x6e, 0x74, 0x2e, 0x45, 0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x53, 0x65, 0x72, 0x76,
	0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1f, 0x2e, 0x65, 0x6e, 0x64, 0x70, 0x6f,
	0x69, 0x6e, 0x74, 0x2e, 0x45, 0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x53, 0x65, 0x72, 0x76,
	0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x23, 0x5a, 0x21, 0x67,
	0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x62, 0x65, 0x61, 0x6d, 0x2d, 0x63,
	0x6c, 0x6f, 0x75, 0x64, 0x2f, 0x62, 0x65, 0x74, 0x61, 0x39, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_endpoint_proto_rawDescOnce sync.Once
	file_endpoint_proto_rawDescData = file_endpoint_proto_rawDesc
)

func file_endpoint_proto_rawDescGZIP() []byte {
	file_endpoint_proto_rawDescOnce.Do(func() {
		file_endpoint_proto_rawDescData = protoimpl.X.CompressGZIP(file_endpoint_proto_rawDescData)
	})
	return file_endpoint_proto_rawDescData
}

var file_endpoint_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_endpoint_proto_goTypes = []interface{}{
	(*EndpointServeRequest)(nil),  // 0: endpoint.EndpointServeRequest
	(*EndpointServeResponse)(nil), // 1: endpoint.EndpointServeResponse
}
var file_endpoint_proto_depIdxs = []int32{
	0, // 0: endpoint.EndpointService.EndpointServe:input_type -> endpoint.EndpointServeRequest
	1, // 1: endpoint.EndpointService.EndpointServe:output_type -> endpoint.EndpointServeResponse
	1, // [1:2] is the sub-list for method output_type
	0, // [0:1] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_endpoint_proto_init() }
func file_endpoint_proto_init() {
	if File_endpoint_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_endpoint_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*EndpointServeRequest); i {
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
		file_endpoint_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*EndpointServeResponse); i {
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
			RawDescriptor: file_endpoint_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_endpoint_proto_goTypes,
		DependencyIndexes: file_endpoint_proto_depIdxs,
		MessageInfos:      file_endpoint_proto_msgTypes,
	}.Build()
	File_endpoint_proto = out.File
	file_endpoint_proto_rawDesc = nil
	file_endpoint_proto_goTypes = nil
	file_endpoint_proto_depIdxs = nil
}
