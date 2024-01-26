// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.32.0
// 	protoc        v4.25.1
// source: container.proto

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

type StopContainerRunRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ContainerId string `protobuf:"bytes,1,opt,name=container_id,json=containerId,proto3" json:"container_id,omitempty"`
}

func (x *StopContainerRunRequest) Reset() {
	*x = StopContainerRunRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_container_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StopContainerRunRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StopContainerRunRequest) ProtoMessage() {}

func (x *StopContainerRunRequest) ProtoReflect() protoreflect.Message {
	mi := &file_container_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StopContainerRunRequest.ProtoReflect.Descriptor instead.
func (*StopContainerRunRequest) Descriptor() ([]byte, []int) {
	return file_container_proto_rawDescGZIP(), []int{0}
}

func (x *StopContainerRunRequest) GetContainerId() string {
	if x != nil {
		return x.ContainerId
	}
	return ""
}

type StopContainerRunResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Success bool   `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *StopContainerRunResponse) Reset() {
	*x = StopContainerRunResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_container_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StopContainerRunResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StopContainerRunResponse) ProtoMessage() {}

func (x *StopContainerRunResponse) ProtoReflect() protoreflect.Message {
	mi := &file_container_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StopContainerRunResponse.ProtoReflect.Descriptor instead.
func (*StopContainerRunResponse) Descriptor() ([]byte, []int) {
	return file_container_proto_rawDescGZIP(), []int{1}
}

func (x *StopContainerRunResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

func (x *StopContainerRunResponse) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

type CommandExecutionRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StubId  string `protobuf:"bytes,1,opt,name=stub_id,json=stubId,proto3" json:"stub_id,omitempty"`
	Command []byte `protobuf:"bytes,2,opt,name=command,proto3" json:"command,omitempty"`
}

func (x *CommandExecutionRequest) Reset() {
	*x = CommandExecutionRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_container_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CommandExecutionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CommandExecutionRequest) ProtoMessage() {}

func (x *CommandExecutionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_container_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CommandExecutionRequest.ProtoReflect.Descriptor instead.
func (*CommandExecutionRequest) Descriptor() ([]byte, []int) {
	return file_container_proto_rawDescGZIP(), []int{2}
}

func (x *CommandExecutionRequest) GetStubId() string {
	if x != nil {
		return x.StubId
	}
	return ""
}

func (x *CommandExecutionRequest) GetCommand() []byte {
	if x != nil {
		return x.Command
	}
	return nil
}

type CommandExecutionResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TaskId   string `protobuf:"bytes,1,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
	Output   string `protobuf:"bytes,2,opt,name=output,proto3" json:"output,omitempty"`
	Done     bool   `protobuf:"varint,3,opt,name=done,proto3" json:"done,omitempty"`
	ExitCode int32  `protobuf:"varint,4,opt,name=exit_code,json=exitCode,proto3" json:"exit_code,omitempty"`
}

func (x *CommandExecutionResponse) Reset() {
	*x = CommandExecutionResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_container_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CommandExecutionResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CommandExecutionResponse) ProtoMessage() {}

func (x *CommandExecutionResponse) ProtoReflect() protoreflect.Message {
	mi := &file_container_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CommandExecutionResponse.ProtoReflect.Descriptor instead.
func (*CommandExecutionResponse) Descriptor() ([]byte, []int) {
	return file_container_proto_rawDescGZIP(), []int{3}
}

func (x *CommandExecutionResponse) GetTaskId() string {
	if x != nil {
		return x.TaskId
	}
	return ""
}

func (x *CommandExecutionResponse) GetOutput() string {
	if x != nil {
		return x.Output
	}
	return ""
}

func (x *CommandExecutionResponse) GetDone() bool {
	if x != nil {
		return x.Done
	}
	return false
}

func (x *CommandExecutionResponse) GetExitCode() int32 {
	if x != nil {
		return x.ExitCode
	}
	return 0
}

var File_container_proto protoreflect.FileDescriptor

var file_container_proto_rawDesc = []byte{
	0x0a, 0x0f, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x09, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x22, 0x3c, 0x0a, 0x17,
	0x53, 0x74, 0x6f, 0x70, 0x43, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x52, 0x75, 0x6e,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x21, 0x0a, 0x0c, 0x63, 0x6f, 0x6e, 0x74, 0x61,
	0x69, 0x6e, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x63,
	0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x49, 0x64, 0x22, 0x4e, 0x0a, 0x18, 0x53, 0x74,
	0x6f, 0x70, 0x43, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x52, 0x75, 0x6e, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73,
	0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73,
	0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0x4c, 0x0a, 0x17, 0x43, 0x6f,
	0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x17, 0x0a, 0x07, 0x73, 0x74, 0x75, 0x62, 0x5f, 0x69, 0x64,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x74, 0x75, 0x62, 0x49, 0x64, 0x12, 0x18,
	0x0a, 0x07, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x07, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x22, 0x7c, 0x0a, 0x18, 0x43, 0x6f, 0x6d, 0x6d,
	0x61, 0x6e, 0x64, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x12, 0x17, 0x0a, 0x07, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x69, 0x64, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x64, 0x12, 0x16, 0x0a,
	0x06, 0x6f, 0x75, 0x74, 0x70, 0x75, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x6f,
	0x75, 0x74, 0x70, 0x75, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x6f, 0x6e, 0x65, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x08, 0x52, 0x04, 0x64, 0x6f, 0x6e, 0x65, 0x12, 0x1b, 0x0a, 0x09, 0x65, 0x78, 0x69,
	0x74, 0x5f, 0x63, 0x6f, 0x64, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x05, 0x52, 0x08, 0x65, 0x78,
	0x69, 0x74, 0x43, 0x6f, 0x64, 0x65, 0x32, 0xcd, 0x01, 0x0a, 0x10, 0x43, 0x6f, 0x6e, 0x74, 0x61,
	0x69, 0x6e, 0x65, 0x72, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x5d, 0x0a, 0x0e, 0x45,
	0x78, 0x65, 0x63, 0x75, 0x74, 0x65, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x12, 0x22, 0x2e,
	0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x2e, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e,
	0x64, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x23, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x2e, 0x43, 0x6f,
	0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x30, 0x01, 0x12, 0x5a, 0x0a, 0x0d, 0x53, 0x74,
	0x6f, 0x70, 0x43, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x12, 0x22, 0x2e, 0x63, 0x6f,
	0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x2e, 0x53, 0x74, 0x6f, 0x70, 0x43, 0x6f, 0x6e, 0x74,
	0x61, 0x69, 0x6e, 0x65, 0x72, 0x52, 0x75, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a,
	0x23, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x2e, 0x53, 0x74, 0x6f, 0x70,
	0x43, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x52, 0x75, 0x6e, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x22, 0x5a, 0x20, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62,
	0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x62, 0x65, 0x61, 0x6d, 0x2d, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2f,
	0x62, 0x65, 0x61, 0x6d, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_container_proto_rawDescOnce sync.Once
	file_container_proto_rawDescData = file_container_proto_rawDesc
)

func file_container_proto_rawDescGZIP() []byte {
	file_container_proto_rawDescOnce.Do(func() {
		file_container_proto_rawDescData = protoimpl.X.CompressGZIP(file_container_proto_rawDescData)
	})
	return file_container_proto_rawDescData
}

var file_container_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_container_proto_goTypes = []interface{}{
	(*StopContainerRunRequest)(nil),  // 0: container.StopContainerRunRequest
	(*StopContainerRunResponse)(nil), // 1: container.StopContainerRunResponse
	(*CommandExecutionRequest)(nil),  // 2: container.CommandExecutionRequest
	(*CommandExecutionResponse)(nil), // 3: container.CommandExecutionResponse
}
var file_container_proto_depIdxs = []int32{
	2, // 0: container.ContainerService.ExecuteCommand:input_type -> container.CommandExecutionRequest
	0, // 1: container.ContainerService.StopContainer:input_type -> container.StopContainerRunRequest
	3, // 2: container.ContainerService.ExecuteCommand:output_type -> container.CommandExecutionResponse
	1, // 3: container.ContainerService.StopContainer:output_type -> container.StopContainerRunResponse
	2, // [2:4] is the sub-list for method output_type
	0, // [0:2] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_container_proto_init() }
func file_container_proto_init() {
	if File_container_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_container_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StopContainerRunRequest); i {
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
		file_container_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StopContainerRunResponse); i {
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
		file_container_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CommandExecutionRequest); i {
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
		file_container_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CommandExecutionResponse); i {
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
			RawDescriptor: file_container_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_container_proto_goTypes,
		DependencyIndexes: file_container_proto_depIdxs,
		MessageInfos:      file_container_proto_msgTypes,
	}.Build()
	File_container_proto = out.File
	file_container_proto_rawDesc = nil
	file_container_proto_goTypes = nil
	file_container_proto_depIdxs = nil
}
