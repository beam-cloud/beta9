// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v4.25.1
// source: queue.proto

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

type SimpleQueuePutRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name  string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *SimpleQueuePutRequest) Reset() {
	*x = SimpleQueuePutRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SimpleQueuePutRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SimpleQueuePutRequest) ProtoMessage() {}

func (x *SimpleQueuePutRequest) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SimpleQueuePutRequest.ProtoReflect.Descriptor instead.
func (*SimpleQueuePutRequest) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{0}
}

func (x *SimpleQueuePutRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *SimpleQueuePutRequest) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

type SimpleQueuePutResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ok bool `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
}

func (x *SimpleQueuePutResponse) Reset() {
	*x = SimpleQueuePutResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SimpleQueuePutResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SimpleQueuePutResponse) ProtoMessage() {}

func (x *SimpleQueuePutResponse) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SimpleQueuePutResponse.ProtoReflect.Descriptor instead.
func (*SimpleQueuePutResponse) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{1}
}

func (x *SimpleQueuePutResponse) GetOk() bool {
	if x != nil {
		return x.Ok
	}
	return false
}

type SimpleQueuePopRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name  string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *SimpleQueuePopRequest) Reset() {
	*x = SimpleQueuePopRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SimpleQueuePopRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SimpleQueuePopRequest) ProtoMessage() {}

func (x *SimpleQueuePopRequest) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SimpleQueuePopRequest.ProtoReflect.Descriptor instead.
func (*SimpleQueuePopRequest) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{2}
}

func (x *SimpleQueuePopRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *SimpleQueuePopRequest) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

type SimpleQueuePopResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ok    bool   `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *SimpleQueuePopResponse) Reset() {
	*x = SimpleQueuePopResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SimpleQueuePopResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SimpleQueuePopResponse) ProtoMessage() {}

func (x *SimpleQueuePopResponse) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SimpleQueuePopResponse.ProtoReflect.Descriptor instead.
func (*SimpleQueuePopResponse) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{3}
}

func (x *SimpleQueuePopResponse) GetOk() bool {
	if x != nil {
		return x.Ok
	}
	return false
}

func (x *SimpleQueuePopResponse) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

type SimpleQueuePeekResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ok    bool   `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *SimpleQueuePeekResponse) Reset() {
	*x = SimpleQueuePeekResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SimpleQueuePeekResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SimpleQueuePeekResponse) ProtoMessage() {}

func (x *SimpleQueuePeekResponse) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SimpleQueuePeekResponse.ProtoReflect.Descriptor instead.
func (*SimpleQueuePeekResponse) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{4}
}

func (x *SimpleQueuePeekResponse) GetOk() bool {
	if x != nil {
		return x.Ok
	}
	return false
}

func (x *SimpleQueuePeekResponse) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

type SimpleQueueEmptyResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ok    bool `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
	Empty bool `protobuf:"varint,2,opt,name=empty,proto3" json:"empty,omitempty"`
}

func (x *SimpleQueueEmptyResponse) Reset() {
	*x = SimpleQueueEmptyResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SimpleQueueEmptyResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SimpleQueueEmptyResponse) ProtoMessage() {}

func (x *SimpleQueueEmptyResponse) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SimpleQueueEmptyResponse.ProtoReflect.Descriptor instead.
func (*SimpleQueueEmptyResponse) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{5}
}

func (x *SimpleQueueEmptyResponse) GetOk() bool {
	if x != nil {
		return x.Ok
	}
	return false
}

func (x *SimpleQueueEmptyResponse) GetEmpty() bool {
	if x != nil {
		return x.Empty
	}
	return false
}

type SimpleQueueSizeResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ok   bool   `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
	Size uint64 `protobuf:"varint,2,opt,name=size,proto3" json:"size,omitempty"`
}

func (x *SimpleQueueSizeResponse) Reset() {
	*x = SimpleQueueSizeResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SimpleQueueSizeResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SimpleQueueSizeResponse) ProtoMessage() {}

func (x *SimpleQueueSizeResponse) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SimpleQueueSizeResponse.ProtoReflect.Descriptor instead.
func (*SimpleQueueSizeResponse) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{6}
}

func (x *SimpleQueueSizeResponse) GetOk() bool {
	if x != nil {
		return x.Ok
	}
	return false
}

func (x *SimpleQueueSizeResponse) GetSize() uint64 {
	if x != nil {
		return x.Size
	}
	return 0
}

type SimpleQueueRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

func (x *SimpleQueueRequest) Reset() {
	*x = SimpleQueueRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SimpleQueueRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SimpleQueueRequest) ProtoMessage() {}

func (x *SimpleQueueRequest) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SimpleQueueRequest.ProtoReflect.Descriptor instead.
func (*SimpleQueueRequest) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{7}
}

func (x *SimpleQueueRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

var File_queue_proto protoreflect.FileDescriptor

var file_queue_proto_rawDesc = []byte{
	0x0a, 0x0b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0b, 0x73,
	0x69, 0x6d, 0x70, 0x6c, 0x65, 0x71, 0x75, 0x65, 0x75, 0x65, 0x22, 0x41, 0x0a, 0x15, 0x53, 0x69,
	0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x75, 0x74, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x28, 0x0a,
	0x16, 0x53, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x75, 0x74, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x6f, 0x6b, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x08, 0x52, 0x02, 0x6f, 0x6b, 0x22, 0x41, 0x0a, 0x15, 0x53, 0x69, 0x6d, 0x70, 0x6c,
	0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x6f, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x6e, 0x61, 0x6d, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x3e, 0x0a, 0x16, 0x53, 0x69,
	0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x6f, 0x70, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x6f, 0x6b, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08,
	0x52, 0x02, 0x6f, 0x6b, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x3f, 0x0a, 0x17, 0x53, 0x69,
	0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x65, 0x65, 0x6b, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x6f, 0x6b, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x08, 0x52, 0x02, 0x6f, 0x6b, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x40, 0x0a, 0x18, 0x53,
	0x69, 0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x6f, 0x6b, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x08, 0x52, 0x02, 0x6f, 0x6b, 0x12, 0x14, 0x0a, 0x05, 0x65, 0x6d, 0x70, 0x74, 0x79,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x05, 0x65, 0x6d, 0x70, 0x74, 0x79, 0x22, 0x3d, 0x0a,
	0x17, 0x53, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x53, 0x69, 0x7a, 0x65,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x6f, 0x6b, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x08, 0x52, 0x02, 0x6f, 0x6b, 0x12, 0x12, 0x0a, 0x04, 0x73, 0x69, 0x7a, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x04, 0x73, 0x69, 0x7a, 0x65, 0x22, 0x28, 0x0a, 0x12,
	0x53, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x32, 0xe4, 0x03, 0x0a, 0x12, 0x53, 0x69, 0x6d, 0x70, 0x6c,
	0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x5b, 0x0a,
	0x0e, 0x53, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x75, 0x74, 0x12,
	0x22, 0x2e, 0x73, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x53, 0x69,
	0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x75, 0x74, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x23, 0x2e, 0x73, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x71, 0x75, 0x65, 0x75,
	0x65, 0x2e, 0x53, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x75, 0x74,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x5b, 0x0a, 0x0e, 0x53, 0x69,
	0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x6f, 0x70, 0x12, 0x22, 0x2e, 0x73,
	0x69, 0x6d, 0x70, 0x6c, 0x65, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x53, 0x69, 0x6d, 0x70, 0x6c,
	0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x6f, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x1a, 0x23, 0x2e, 0x73, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x53,
	0x69, 0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x6f, 0x70, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x5a, 0x0a, 0x0f, 0x53, 0x69, 0x6d, 0x70, 0x6c,
	0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x65, 0x65, 0x6b, 0x12, 0x1f, 0x2e, 0x73, 0x69, 0x6d,
	0x70, 0x6c, 0x65, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x53, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x51,
	0x75, 0x65, 0x75, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x24, 0x2e, 0x73, 0x69,
	0x6d, 0x70, 0x6c, 0x65, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x53, 0x69, 0x6d, 0x70, 0x6c, 0x65,
	0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x65, 0x65, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x22, 0x00, 0x12, 0x5c, 0x0a, 0x10, 0x53, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65,
	0x75, 0x65, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x12, 0x1f, 0x2e, 0x73, 0x69, 0x6d, 0x70, 0x6c, 0x65,
	0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x53, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75,
	0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x25, 0x2e, 0x73, 0x69, 0x6d, 0x70, 0x6c,
	0x65, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x53, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65,
	0x75, 0x65, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22,
	0x00, 0x12, 0x5a, 0x0a, 0x0f, 0x53, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65,
	0x53, 0x69, 0x7a, 0x65, 0x12, 0x1f, 0x2e, 0x73, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x71, 0x75, 0x65,
	0x75, 0x65, 0x2e, 0x53, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x24, 0x2e, 0x73, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x71, 0x75,
	0x65, 0x75, 0x65, 0x2e, 0x53, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x53,
	0x69, 0x7a, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x23, 0x5a,
	0x21, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x62, 0x65, 0x61, 0x6d,
	0x2d, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2f, 0x62, 0x65, 0x74, 0x61, 0x39, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_queue_proto_rawDescOnce sync.Once
	file_queue_proto_rawDescData = file_queue_proto_rawDesc
)

func file_queue_proto_rawDescGZIP() []byte {
	file_queue_proto_rawDescOnce.Do(func() {
		file_queue_proto_rawDescData = protoimpl.X.CompressGZIP(file_queue_proto_rawDescData)
	})
	return file_queue_proto_rawDescData
}

var file_queue_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_queue_proto_goTypes = []interface{}{
	(*SimpleQueuePutRequest)(nil),    // 0: simplequeue.SimpleQueuePutRequest
	(*SimpleQueuePutResponse)(nil),   // 1: simplequeue.SimpleQueuePutResponse
	(*SimpleQueuePopRequest)(nil),    // 2: simplequeue.SimpleQueuePopRequest
	(*SimpleQueuePopResponse)(nil),   // 3: simplequeue.SimpleQueuePopResponse
	(*SimpleQueuePeekResponse)(nil),  // 4: simplequeue.SimpleQueuePeekResponse
	(*SimpleQueueEmptyResponse)(nil), // 5: simplequeue.SimpleQueueEmptyResponse
	(*SimpleQueueSizeResponse)(nil),  // 6: simplequeue.SimpleQueueSizeResponse
	(*SimpleQueueRequest)(nil),       // 7: simplequeue.SimpleQueueRequest
}
var file_queue_proto_depIdxs = []int32{
	0, // 0: simplequeue.SimpleQueueService.SimpleQueuePut:input_type -> simplequeue.SimpleQueuePutRequest
	2, // 1: simplequeue.SimpleQueueService.SimpleQueuePop:input_type -> simplequeue.SimpleQueuePopRequest
	7, // 2: simplequeue.SimpleQueueService.SimpleQueuePeek:input_type -> simplequeue.SimpleQueueRequest
	7, // 3: simplequeue.SimpleQueueService.SimpleQueueEmpty:input_type -> simplequeue.SimpleQueueRequest
	7, // 4: simplequeue.SimpleQueueService.SimpleQueueSize:input_type -> simplequeue.SimpleQueueRequest
	1, // 5: simplequeue.SimpleQueueService.SimpleQueuePut:output_type -> simplequeue.SimpleQueuePutResponse
	3, // 6: simplequeue.SimpleQueueService.SimpleQueuePop:output_type -> simplequeue.SimpleQueuePopResponse
	4, // 7: simplequeue.SimpleQueueService.SimpleQueuePeek:output_type -> simplequeue.SimpleQueuePeekResponse
	5, // 8: simplequeue.SimpleQueueService.SimpleQueueEmpty:output_type -> simplequeue.SimpleQueueEmptyResponse
	6, // 9: simplequeue.SimpleQueueService.SimpleQueueSize:output_type -> simplequeue.SimpleQueueSizeResponse
	5, // [5:10] is the sub-list for method output_type
	0, // [0:5] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_queue_proto_init() }
func file_queue_proto_init() {
	if File_queue_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_queue_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SimpleQueuePutRequest); i {
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
		file_queue_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SimpleQueuePutResponse); i {
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
		file_queue_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SimpleQueuePopRequest); i {
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
		file_queue_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SimpleQueuePopResponse); i {
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
		file_queue_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SimpleQueuePeekResponse); i {
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
		file_queue_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SimpleQueueEmptyResponse); i {
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
		file_queue_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SimpleQueueSizeResponse); i {
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
		file_queue_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SimpleQueueRequest); i {
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
			RawDescriptor: file_queue_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_queue_proto_goTypes,
		DependencyIndexes: file_queue_proto_depIdxs,
		MessageInfos:      file_queue_proto_msgTypes,
	}.Build()
	File_queue_proto = out.File
	file_queue_proto_rawDesc = nil
	file_queue_proto_goTypes = nil
	file_queue_proto_depIdxs = nil
}
