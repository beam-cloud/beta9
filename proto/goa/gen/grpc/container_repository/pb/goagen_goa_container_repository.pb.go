// Code generated with goa v3.19.1, DO NOT EDIT.
//
// ContainerRepository protocol buffer definition
//
// Command:
// $ goa gen github.com/beam-cloud/beta9/pkg/repository/dsl -o proto/goa

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v4.25.1
// source: goagen_goa_container_repository.proto

package container_repositorypb

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

type GetContainerStateRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// ID of the container
	ContainerId string `protobuf:"bytes,1,opt,name=container_id,json=containerId,proto3" json:"container_id,omitempty"`
}

func (x *GetContainerStateRequest) Reset() {
	*x = GetContainerStateRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_goagen_goa_container_repository_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetContainerStateRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetContainerStateRequest) ProtoMessage() {}

func (x *GetContainerStateRequest) ProtoReflect() protoreflect.Message {
	mi := &file_goagen_goa_container_repository_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetContainerStateRequest.ProtoReflect.Descriptor instead.
func (*GetContainerStateRequest) Descriptor() ([]byte, []int) {
	return file_goagen_goa_container_repository_proto_rawDescGZIP(), []int{0}
}

func (x *GetContainerStateRequest) GetContainerId() string {
	if x != nil {
		return x.ContainerId
	}
	return ""
}

type GetContainerStateResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *GetContainerStateResponse) Reset() {
	*x = GetContainerStateResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_goagen_goa_container_repository_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetContainerStateResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetContainerStateResponse) ProtoMessage() {}

func (x *GetContainerStateResponse) ProtoReflect() protoreflect.Message {
	mi := &file_goagen_goa_container_repository_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetContainerStateResponse.ProtoReflect.Descriptor instead.
func (*GetContainerStateResponse) Descriptor() ([]byte, []int) {
	return file_goagen_goa_container_repository_proto_rawDescGZIP(), []int{1}
}

type SetContainerStateRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// ID of the container
	ContainerId *string `protobuf:"bytes,1,opt,name=container_id,json=containerId,proto3,oneof" json:"container_id,omitempty"`
	// Container state information
	State *ContainerState `protobuf:"bytes,2,opt,name=state,proto3" json:"state,omitempty"`
}

func (x *SetContainerStateRequest) Reset() {
	*x = SetContainerStateRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_goagen_goa_container_repository_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SetContainerStateRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SetContainerStateRequest) ProtoMessage() {}

func (x *SetContainerStateRequest) ProtoReflect() protoreflect.Message {
	mi := &file_goagen_goa_container_repository_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SetContainerStateRequest.ProtoReflect.Descriptor instead.
func (*SetContainerStateRequest) Descriptor() ([]byte, []int) {
	return file_goagen_goa_container_repository_proto_rawDescGZIP(), []int{2}
}

func (x *SetContainerStateRequest) GetContainerId() string {
	if x != nil && x.ContainerId != nil {
		return *x.ContainerId
	}
	return ""
}

func (x *SetContainerStateRequest) GetState() *ContainerState {
	if x != nil {
		return x.State
	}
	return nil
}

// A container state
type ContainerState struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *ContainerState) Reset() {
	*x = ContainerState{}
	if protoimpl.UnsafeEnabled {
		mi := &file_goagen_goa_container_repository_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ContainerState) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ContainerState) ProtoMessage() {}

func (x *ContainerState) ProtoReflect() protoreflect.Message {
	mi := &file_goagen_goa_container_repository_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ContainerState.ProtoReflect.Descriptor instead.
func (*ContainerState) Descriptor() ([]byte, []int) {
	return file_goagen_goa_container_repository_proto_rawDescGZIP(), []int{3}
}

type SetContainerStateResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *SetContainerStateResponse) Reset() {
	*x = SetContainerStateResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_goagen_goa_container_repository_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SetContainerStateResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SetContainerStateResponse) ProtoMessage() {}

func (x *SetContainerStateResponse) ProtoReflect() protoreflect.Message {
	mi := &file_goagen_goa_container_repository_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SetContainerStateResponse.ProtoReflect.Descriptor instead.
func (*SetContainerStateResponse) Descriptor() ([]byte, []int) {
	return file_goagen_goa_container_repository_proto_rawDescGZIP(), []int{4}
}

var File_goagen_goa_container_repository_proto protoreflect.FileDescriptor

var file_goagen_goa_container_repository_proto_rawDesc = []byte{
	0x0a, 0x25, 0x67, 0x6f, 0x61, 0x67, 0x65, 0x6e, 0x5f, 0x67, 0x6f, 0x61, 0x5f, 0x63, 0x6f, 0x6e,
	0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x5f, 0x72, 0x65, 0x70, 0x6f, 0x73, 0x69, 0x74, 0x6f, 0x72,
	0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x14, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e,
	0x65, 0x72, 0x5f, 0x72, 0x65, 0x70, 0x6f, 0x73, 0x69, 0x74, 0x6f, 0x72, 0x79, 0x22, 0x3d, 0x0a,
	0x18, 0x47, 0x65, 0x74, 0x43, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x53, 0x74, 0x61,
	0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x21, 0x0a, 0x0c, 0x63, 0x6f, 0x6e,
	0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0b, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x49, 0x64, 0x22, 0x1b, 0x0a, 0x19,
	0x47, 0x65, 0x74, 0x43, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x53, 0x74, 0x61, 0x74,
	0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x8f, 0x01, 0x0a, 0x18, 0x53, 0x65,
	0x74, 0x43, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x53, 0x74, 0x61, 0x74, 0x65, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x26, 0x0a, 0x0c, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69,
	0x6e, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x0b,
	0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x49, 0x64, 0x88, 0x01, 0x01, 0x12, 0x3a,
	0x0a, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x24, 0x2e,
	0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x5f, 0x72, 0x65, 0x70, 0x6f, 0x73, 0x69,
	0x74, 0x6f, 0x72, 0x79, 0x2e, 0x43, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x53, 0x74,
	0x61, 0x74, 0x65, 0x52, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x42, 0x0f, 0x0a, 0x0d, 0x5f, 0x63,
	0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x22, 0x10, 0x0a, 0x0e, 0x43,
	0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x53, 0x74, 0x61, 0x74, 0x65, 0x22, 0x1b, 0x0a,
	0x19, 0x53, 0x65, 0x74, 0x43, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x53, 0x74, 0x61,
	0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x32, 0x81, 0x02, 0x0a, 0x13, 0x43,
	0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x52, 0x65, 0x70, 0x6f, 0x73, 0x69, 0x74, 0x6f,
	0x72, 0x79, 0x12, 0x74, 0x0a, 0x11, 0x47, 0x65, 0x74, 0x43, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e,
	0x65, 0x72, 0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x2e, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69,
	0x6e, 0x65, 0x72, 0x5f, 0x72, 0x65, 0x70, 0x6f, 0x73, 0x69, 0x74, 0x6f, 0x72, 0x79, 0x2e, 0x47,
	0x65, 0x74, 0x43, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x53, 0x74, 0x61, 0x74, 0x65,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x2f, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69,
	0x6e, 0x65, 0x72, 0x5f, 0x72, 0x65, 0x70, 0x6f, 0x73, 0x69, 0x74, 0x6f, 0x72, 0x79, 0x2e, 0x47,
	0x65, 0x74, 0x43, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x53, 0x74, 0x61, 0x74, 0x65,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x74, 0x0a, 0x11, 0x53, 0x65, 0x74, 0x43,
	0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x2e, 0x2e,
	0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x5f, 0x72, 0x65, 0x70, 0x6f, 0x73, 0x69,
	0x74, 0x6f, 0x72, 0x79, 0x2e, 0x53, 0x65, 0x74, 0x43, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65,
	0x72, 0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x2f, 0x2e,
	0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x5f, 0x72, 0x65, 0x70, 0x6f, 0x73, 0x69,
	0x74, 0x6f, 0x72, 0x79, 0x2e, 0x53, 0x65, 0x74, 0x43, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65,
	0x72, 0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x42, 0x19,
	0x5a, 0x17, 0x2f, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x5f, 0x72, 0x65, 0x70,
	0x6f, 0x73, 0x69, 0x74, 0x6f, 0x72, 0x79, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_goagen_goa_container_repository_proto_rawDescOnce sync.Once
	file_goagen_goa_container_repository_proto_rawDescData = file_goagen_goa_container_repository_proto_rawDesc
)

func file_goagen_goa_container_repository_proto_rawDescGZIP() []byte {
	file_goagen_goa_container_repository_proto_rawDescOnce.Do(func() {
		file_goagen_goa_container_repository_proto_rawDescData = protoimpl.X.CompressGZIP(file_goagen_goa_container_repository_proto_rawDescData)
	})
	return file_goagen_goa_container_repository_proto_rawDescData
}

var file_goagen_goa_container_repository_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_goagen_goa_container_repository_proto_goTypes = []interface{}{
	(*GetContainerStateRequest)(nil),  // 0: container_repository.GetContainerStateRequest
	(*GetContainerStateResponse)(nil), // 1: container_repository.GetContainerStateResponse
	(*SetContainerStateRequest)(nil),  // 2: container_repository.SetContainerStateRequest
	(*ContainerState)(nil),            // 3: container_repository.ContainerState
	(*SetContainerStateResponse)(nil), // 4: container_repository.SetContainerStateResponse
}
var file_goagen_goa_container_repository_proto_depIdxs = []int32{
	3, // 0: container_repository.SetContainerStateRequest.state:type_name -> container_repository.ContainerState
	0, // 1: container_repository.ContainerRepository.GetContainerState:input_type -> container_repository.GetContainerStateRequest
	2, // 2: container_repository.ContainerRepository.SetContainerState:input_type -> container_repository.SetContainerStateRequest
	1, // 3: container_repository.ContainerRepository.GetContainerState:output_type -> container_repository.GetContainerStateResponse
	4, // 4: container_repository.ContainerRepository.SetContainerState:output_type -> container_repository.SetContainerStateResponse
	3, // [3:5] is the sub-list for method output_type
	1, // [1:3] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_goagen_goa_container_repository_proto_init() }
func file_goagen_goa_container_repository_proto_init() {
	if File_goagen_goa_container_repository_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_goagen_goa_container_repository_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetContainerStateRequest); i {
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
		file_goagen_goa_container_repository_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetContainerStateResponse); i {
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
		file_goagen_goa_container_repository_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SetContainerStateRequest); i {
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
		file_goagen_goa_container_repository_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ContainerState); i {
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
		file_goagen_goa_container_repository_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SetContainerStateResponse); i {
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
	file_goagen_goa_container_repository_proto_msgTypes[2].OneofWrappers = []interface{}{}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_goagen_goa_container_repository_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_goagen_goa_container_repository_proto_goTypes,
		DependencyIndexes: file_goagen_goa_container_repository_proto_depIdxs,
		MessageInfos:      file_goagen_goa_container_repository_proto_msgTypes,
	}.Build()
	File_goagen_goa_container_repository_proto = out.File
	file_goagen_goa_container_repository_proto_rawDesc = nil
	file_goagen_goa_container_repository_proto_goTypes = nil
	file_goagen_goa_container_repository_proto_depIdxs = nil
}
