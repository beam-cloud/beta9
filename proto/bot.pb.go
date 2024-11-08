// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v4.25.1
// source: bot.proto

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

type StartBotServeRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StubId  string `protobuf:"bytes,1,opt,name=stub_id,json=stubId,proto3" json:"stub_id,omitempty"`
	Timeout int32  `protobuf:"varint,2,opt,name=timeout,proto3" json:"timeout,omitempty"`
}

func (x *StartBotServeRequest) Reset() {
	*x = StartBotServeRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bot_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StartBotServeRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StartBotServeRequest) ProtoMessage() {}

func (x *StartBotServeRequest) ProtoReflect() protoreflect.Message {
	mi := &file_bot_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StartBotServeRequest.ProtoReflect.Descriptor instead.
func (*StartBotServeRequest) Descriptor() ([]byte, []int) {
	return file_bot_proto_rawDescGZIP(), []int{0}
}

func (x *StartBotServeRequest) GetStubId() string {
	if x != nil {
		return x.StubId
	}
	return ""
}

func (x *StartBotServeRequest) GetTimeout() int32 {
	if x != nil {
		return x.Timeout
	}
	return 0
}

type StartBotServeResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ok bool `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
}

func (x *StartBotServeResponse) Reset() {
	*x = StartBotServeResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bot_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StartBotServeResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StartBotServeResponse) ProtoMessage() {}

func (x *StartBotServeResponse) ProtoReflect() protoreflect.Message {
	mi := &file_bot_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StartBotServeResponse.ProtoReflect.Descriptor instead.
func (*StartBotServeResponse) Descriptor() ([]byte, []int) {
	return file_bot_proto_rawDescGZIP(), []int{1}
}

func (x *StartBotServeResponse) GetOk() bool {
	if x != nil {
		return x.Ok
	}
	return false
}

type BotServeKeepAliveRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StubId  string `protobuf:"bytes,1,opt,name=stub_id,json=stubId,proto3" json:"stub_id,omitempty"`
	Timeout int32  `protobuf:"varint,2,opt,name=timeout,proto3" json:"timeout,omitempty"`
}

func (x *BotServeKeepAliveRequest) Reset() {
	*x = BotServeKeepAliveRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bot_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BotServeKeepAliveRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BotServeKeepAliveRequest) ProtoMessage() {}

func (x *BotServeKeepAliveRequest) ProtoReflect() protoreflect.Message {
	mi := &file_bot_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BotServeKeepAliveRequest.ProtoReflect.Descriptor instead.
func (*BotServeKeepAliveRequest) Descriptor() ([]byte, []int) {
	return file_bot_proto_rawDescGZIP(), []int{2}
}

func (x *BotServeKeepAliveRequest) GetStubId() string {
	if x != nil {
		return x.StubId
	}
	return ""
}

func (x *BotServeKeepAliveRequest) GetTimeout() int32 {
	if x != nil {
		return x.Timeout
	}
	return 0
}

type BotServeKeepAliveResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ok bool `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
}

func (x *BotServeKeepAliveResponse) Reset() {
	*x = BotServeKeepAliveResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bot_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BotServeKeepAliveResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BotServeKeepAliveResponse) ProtoMessage() {}

func (x *BotServeKeepAliveResponse) ProtoReflect() protoreflect.Message {
	mi := &file_bot_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BotServeKeepAliveResponse.ProtoReflect.Descriptor instead.
func (*BotServeKeepAliveResponse) Descriptor() ([]byte, []int) {
	return file_bot_proto_rawDescGZIP(), []int{3}
}

func (x *BotServeKeepAliveResponse) GetOk() bool {
	if x != nil {
		return x.Ok
	}
	return false
}

type PopBotTaskRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StubId         string `protobuf:"bytes,1,opt,name=stub_id,json=stubId,proto3" json:"stub_id,omitempty"`
	SessionId      string `protobuf:"bytes,2,opt,name=session_id,json=sessionId,proto3" json:"session_id,omitempty"`
	TransitionName string `protobuf:"bytes,3,opt,name=transition_name,json=transitionName,proto3" json:"transition_name,omitempty"`
}

func (x *PopBotTaskRequest) Reset() {
	*x = PopBotTaskRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bot_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PopBotTaskRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PopBotTaskRequest) ProtoMessage() {}

func (x *PopBotTaskRequest) ProtoReflect() protoreflect.Message {
	mi := &file_bot_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PopBotTaskRequest.ProtoReflect.Descriptor instead.
func (*PopBotTaskRequest) Descriptor() ([]byte, []int) {
	return file_bot_proto_rawDescGZIP(), []int{4}
}

func (x *PopBotTaskRequest) GetStubId() string {
	if x != nil {
		return x.StubId
	}
	return ""
}

func (x *PopBotTaskRequest) GetSessionId() string {
	if x != nil {
		return x.SessionId
	}
	return ""
}

func (x *PopBotTaskRequest) GetTransitionName() string {
	if x != nil {
		return x.TransitionName
	}
	return ""
}

type PopBotTaskResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ok      bool                                      `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
	Markers map[string]*PopBotTaskResponse_MarkerList `protobuf:"bytes,2,rep,name=markers,proto3" json:"markers,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *PopBotTaskResponse) Reset() {
	*x = PopBotTaskResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bot_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PopBotTaskResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PopBotTaskResponse) ProtoMessage() {}

func (x *PopBotTaskResponse) ProtoReflect() protoreflect.Message {
	mi := &file_bot_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PopBotTaskResponse.ProtoReflect.Descriptor instead.
func (*PopBotTaskResponse) Descriptor() ([]byte, []int) {
	return file_bot_proto_rawDescGZIP(), []int{5}
}

func (x *PopBotTaskResponse) GetOk() bool {
	if x != nil {
		return x.Ok
	}
	return false
}

func (x *PopBotTaskResponse) GetMarkers() map[string]*PopBotTaskResponse_MarkerList {
	if x != nil {
		return x.Markers
	}
	return nil
}

type MarkerField struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	FieldName  string `protobuf:"bytes,1,opt,name=field_name,json=fieldName,proto3" json:"field_name,omitempty"`
	FieldValue string `protobuf:"bytes,2,opt,name=field_value,json=fieldValue,proto3" json:"field_value,omitempty"`
}

func (x *MarkerField) Reset() {
	*x = MarkerField{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bot_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MarkerField) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MarkerField) ProtoMessage() {}

func (x *MarkerField) ProtoReflect() protoreflect.Message {
	mi := &file_bot_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MarkerField.ProtoReflect.Descriptor instead.
func (*MarkerField) Descriptor() ([]byte, []int) {
	return file_bot_proto_rawDescGZIP(), []int{6}
}

func (x *MarkerField) GetFieldName() string {
	if x != nil {
		return x.FieldName
	}
	return ""
}

func (x *MarkerField) GetFieldValue() string {
	if x != nil {
		return x.FieldValue
	}
	return ""
}

type Marker struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	LocationName string         `protobuf:"bytes,1,opt,name=location_name,json=locationName,proto3" json:"location_name,omitempty"`
	Fields       []*MarkerField `protobuf:"bytes,2,rep,name=fields,proto3" json:"fields,omitempty"`
}

func (x *Marker) Reset() {
	*x = Marker{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bot_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Marker) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Marker) ProtoMessage() {}

func (x *Marker) ProtoReflect() protoreflect.Message {
	mi := &file_bot_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Marker.ProtoReflect.Descriptor instead.
func (*Marker) Descriptor() ([]byte, []int) {
	return file_bot_proto_rawDescGZIP(), []int{7}
}

func (x *Marker) GetLocationName() string {
	if x != nil {
		return x.LocationName
	}
	return ""
}

func (x *Marker) GetFields() []*MarkerField {
	if x != nil {
		return x.Fields
	}
	return nil
}

type PushBotMarkersRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StubId    string                                       `protobuf:"bytes,1,opt,name=stub_id,json=stubId,proto3" json:"stub_id,omitempty"`
	SessionId string                                       `protobuf:"bytes,2,opt,name=session_id,json=sessionId,proto3" json:"session_id,omitempty"`
	Markers   map[string]*PushBotMarkersRequest_MarkerList `protobuf:"bytes,3,rep,name=markers,proto3" json:"markers,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *PushBotMarkersRequest) Reset() {
	*x = PushBotMarkersRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bot_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PushBotMarkersRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PushBotMarkersRequest) ProtoMessage() {}

func (x *PushBotMarkersRequest) ProtoReflect() protoreflect.Message {
	mi := &file_bot_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PushBotMarkersRequest.ProtoReflect.Descriptor instead.
func (*PushBotMarkersRequest) Descriptor() ([]byte, []int) {
	return file_bot_proto_rawDescGZIP(), []int{8}
}

func (x *PushBotMarkersRequest) GetStubId() string {
	if x != nil {
		return x.StubId
	}
	return ""
}

func (x *PushBotMarkersRequest) GetSessionId() string {
	if x != nil {
		return x.SessionId
	}
	return ""
}

func (x *PushBotMarkersRequest) GetMarkers() map[string]*PushBotMarkersRequest_MarkerList {
	if x != nil {
		return x.Markers
	}
	return nil
}

type PushBotMarkersResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ok bool `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
}

func (x *PushBotMarkersResponse) Reset() {
	*x = PushBotMarkersResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bot_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PushBotMarkersResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PushBotMarkersResponse) ProtoMessage() {}

func (x *PushBotMarkersResponse) ProtoReflect() protoreflect.Message {
	mi := &file_bot_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PushBotMarkersResponse.ProtoReflect.Descriptor instead.
func (*PushBotMarkersResponse) Descriptor() ([]byte, []int) {
	return file_bot_proto_rawDescGZIP(), []int{9}
}

func (x *PushBotMarkersResponse) GetOk() bool {
	if x != nil {
		return x.Ok
	}
	return false
}

type PushBotEventRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StubId     string `protobuf:"bytes,1,opt,name=stub_id,json=stubId,proto3" json:"stub_id,omitempty"`
	SessionId  string `protobuf:"bytes,2,opt,name=session_id,json=sessionId,proto3" json:"session_id,omitempty"`
	EventType  string `protobuf:"bytes,3,opt,name=event_type,json=eventType,proto3" json:"event_type,omitempty"`
	EventValue string `protobuf:"bytes,4,opt,name=event_value,json=eventValue,proto3" json:"event_value,omitempty"`
}

func (x *PushBotEventRequest) Reset() {
	*x = PushBotEventRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bot_proto_msgTypes[10]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PushBotEventRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PushBotEventRequest) ProtoMessage() {}

func (x *PushBotEventRequest) ProtoReflect() protoreflect.Message {
	mi := &file_bot_proto_msgTypes[10]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PushBotEventRequest.ProtoReflect.Descriptor instead.
func (*PushBotEventRequest) Descriptor() ([]byte, []int) {
	return file_bot_proto_rawDescGZIP(), []int{10}
}

func (x *PushBotEventRequest) GetStubId() string {
	if x != nil {
		return x.StubId
	}
	return ""
}

func (x *PushBotEventRequest) GetSessionId() string {
	if x != nil {
		return x.SessionId
	}
	return ""
}

func (x *PushBotEventRequest) GetEventType() string {
	if x != nil {
		return x.EventType
	}
	return ""
}

func (x *PushBotEventRequest) GetEventValue() string {
	if x != nil {
		return x.EventValue
	}
	return ""
}

type PushBotEventResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ok bool `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
}

func (x *PushBotEventResponse) Reset() {
	*x = PushBotEventResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bot_proto_msgTypes[11]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PushBotEventResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PushBotEventResponse) ProtoMessage() {}

func (x *PushBotEventResponse) ProtoReflect() protoreflect.Message {
	mi := &file_bot_proto_msgTypes[11]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PushBotEventResponse.ProtoReflect.Descriptor instead.
func (*PushBotEventResponse) Descriptor() ([]byte, []int) {
	return file_bot_proto_rawDescGZIP(), []int{11}
}

func (x *PushBotEventResponse) GetOk() bool {
	if x != nil {
		return x.Ok
	}
	return false
}

type PopBotTaskResponse_MarkerList struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Markers []*Marker `protobuf:"bytes,1,rep,name=markers,proto3" json:"markers,omitempty"`
}

func (x *PopBotTaskResponse_MarkerList) Reset() {
	*x = PopBotTaskResponse_MarkerList{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bot_proto_msgTypes[13]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PopBotTaskResponse_MarkerList) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PopBotTaskResponse_MarkerList) ProtoMessage() {}

func (x *PopBotTaskResponse_MarkerList) ProtoReflect() protoreflect.Message {
	mi := &file_bot_proto_msgTypes[13]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PopBotTaskResponse_MarkerList.ProtoReflect.Descriptor instead.
func (*PopBotTaskResponse_MarkerList) Descriptor() ([]byte, []int) {
	return file_bot_proto_rawDescGZIP(), []int{5, 1}
}

func (x *PopBotTaskResponse_MarkerList) GetMarkers() []*Marker {
	if x != nil {
		return x.Markers
	}
	return nil
}

type PushBotMarkersRequest_MarkerList struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Markers []*Marker `protobuf:"bytes,4,rep,name=markers,proto3" json:"markers,omitempty"`
}

func (x *PushBotMarkersRequest_MarkerList) Reset() {
	*x = PushBotMarkersRequest_MarkerList{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bot_proto_msgTypes[15]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PushBotMarkersRequest_MarkerList) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PushBotMarkersRequest_MarkerList) ProtoMessage() {}

func (x *PushBotMarkersRequest_MarkerList) ProtoReflect() protoreflect.Message {
	mi := &file_bot_proto_msgTypes[15]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PushBotMarkersRequest_MarkerList.ProtoReflect.Descriptor instead.
func (*PushBotMarkersRequest_MarkerList) Descriptor() ([]byte, []int) {
	return file_bot_proto_rawDescGZIP(), []int{8, 1}
}

func (x *PushBotMarkersRequest_MarkerList) GetMarkers() []*Marker {
	if x != nil {
		return x.Markers
	}
	return nil
}

var File_bot_proto protoreflect.FileDescriptor

var file_bot_proto_rawDesc = []byte{
	0x0a, 0x09, 0x62, 0x6f, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x03, 0x62, 0x6f, 0x74,
	0x22, 0x49, 0x0a, 0x14, 0x53, 0x74, 0x61, 0x72, 0x74, 0x42, 0x6f, 0x74, 0x53, 0x65, 0x72, 0x76,
	0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x17, 0x0a, 0x07, 0x73, 0x74, 0x75, 0x62,
	0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x74, 0x75, 0x62, 0x49,
	0x64, 0x12, 0x18, 0x0a, 0x07, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x05, 0x52, 0x07, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x22, 0x27, 0x0a, 0x15, 0x53,
	0x74, 0x61, 0x72, 0x74, 0x42, 0x6f, 0x74, 0x53, 0x65, 0x72, 0x76, 0x65, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x6f, 0x6b, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08,
	0x52, 0x02, 0x6f, 0x6b, 0x22, 0x4d, 0x0a, 0x18, 0x42, 0x6f, 0x74, 0x53, 0x65, 0x72, 0x76, 0x65,
	0x4b, 0x65, 0x65, 0x70, 0x41, 0x6c, 0x69, 0x76, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x17, 0x0a, 0x07, 0x73, 0x74, 0x75, 0x62, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x06, 0x73, 0x74, 0x75, 0x62, 0x49, 0x64, 0x12, 0x18, 0x0a, 0x07, 0x74, 0x69, 0x6d,
	0x65, 0x6f, 0x75, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x07, 0x74, 0x69, 0x6d, 0x65,
	0x6f, 0x75, 0x74, 0x22, 0x2b, 0x0a, 0x19, 0x42, 0x6f, 0x74, 0x53, 0x65, 0x72, 0x76, 0x65, 0x4b,
	0x65, 0x65, 0x70, 0x41, 0x6c, 0x69, 0x76, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x12, 0x0e, 0x0a, 0x02, 0x6f, 0x6b, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x02, 0x6f, 0x6b,
	0x22, 0x74, 0x0a, 0x11, 0x50, 0x6f, 0x70, 0x42, 0x6f, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x17, 0x0a, 0x07, 0x73, 0x74, 0x75, 0x62, 0x5f, 0x69, 0x64,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x74, 0x75, 0x62, 0x49, 0x64, 0x12, 0x1d,
	0x0a, 0x0a, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x09, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x12, 0x27, 0x0a,
	0x0f, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x6e, 0x61, 0x6d, 0x65,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0e, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x69, 0x74, 0x69,
	0x6f, 0x6e, 0x4e, 0x61, 0x6d, 0x65, 0x22, 0xf9, 0x01, 0x0a, 0x12, 0x50, 0x6f, 0x70, 0x42, 0x6f,
	0x74, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a,
	0x02, 0x6f, 0x6b, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x02, 0x6f, 0x6b, 0x12, 0x3e, 0x0a,
	0x07, 0x6d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x24,
	0x2e, 0x62, 0x6f, 0x74, 0x2e, 0x50, 0x6f, 0x70, 0x42, 0x6f, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x2e, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x52, 0x07, 0x6d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x1a, 0x5e, 0x0a,
	0x0c, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a,
	0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12,
	0x38, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x22,
	0x2e, 0x62, 0x6f, 0x74, 0x2e, 0x50, 0x6f, 0x70, 0x42, 0x6f, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x2e, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x4c, 0x69,
	0x73, 0x74, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x1a, 0x33, 0x0a,
	0x0a, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x4c, 0x69, 0x73, 0x74, 0x12, 0x25, 0x0a, 0x07, 0x6d,
	0x61, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x62,
	0x6f, 0x74, 0x2e, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x52, 0x07, 0x6d, 0x61, 0x72, 0x6b, 0x65,
	0x72, 0x73, 0x22, 0x4d, 0x0a, 0x0b, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x46, 0x69, 0x65, 0x6c,
	0x64, 0x12, 0x1d, 0x0a, 0x0a, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x4e, 0x61, 0x6d, 0x65,
	0x12, 0x1f, 0x0a, 0x0b, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x5f, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x56, 0x61, 0x6c, 0x75,
	0x65, 0x22, 0x57, 0x0a, 0x06, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x12, 0x23, 0x0a, 0x0d, 0x6c,
	0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0c, 0x6c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x4e, 0x61, 0x6d, 0x65,
	0x12, 0x28, 0x0a, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x10, 0x2e, 0x62, 0x6f, 0x74, 0x2e, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x46, 0x69, 0x65,
	0x6c, 0x64, 0x52, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x73, 0x22, 0xaa, 0x02, 0x0a, 0x15, 0x50,
	0x75, 0x73, 0x68, 0x42, 0x6f, 0x74, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x17, 0x0a, 0x07, 0x73, 0x74, 0x75, 0x62, 0x5f, 0x69, 0x64, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x74, 0x75, 0x62, 0x49, 0x64, 0x12, 0x1d, 0x0a,
	0x0a, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x09, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x12, 0x41, 0x0a, 0x07,
	0x6d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x27, 0x2e,
	0x62, 0x6f, 0x74, 0x2e, 0x50, 0x75, 0x73, 0x68, 0x42, 0x6f, 0x74, 0x4d, 0x61, 0x72, 0x6b, 0x65,
	0x72, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72,
	0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x07, 0x6d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x1a,
	0x61, 0x0a, 0x0c, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12,
	0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65,
	0x79, 0x12, 0x3b, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x25, 0x2e, 0x62, 0x6f, 0x74, 0x2e, 0x50, 0x75, 0x73, 0x68, 0x42, 0x6f, 0x74, 0x4d, 0x61,
	0x72, 0x6b, 0x65, 0x72, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x4d, 0x61, 0x72,
	0x6b, 0x65, 0x72, 0x4c, 0x69, 0x73, 0x74, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02,
	0x38, 0x01, 0x1a, 0x33, 0x0a, 0x0a, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x4c, 0x69, 0x73, 0x74,
	0x12, 0x25, 0x0a, 0x07, 0x6d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x0b, 0x2e, 0x62, 0x6f, 0x74, 0x2e, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x52, 0x07,
	0x6d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x22, 0x28, 0x0a, 0x16, 0x50, 0x75, 0x73, 0x68, 0x42,
	0x6f, 0x74, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x12, 0x0e, 0x0a, 0x02, 0x6f, 0x6b, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x02, 0x6f,
	0x6b, 0x22, 0x8d, 0x01, 0x0a, 0x13, 0x50, 0x75, 0x73, 0x68, 0x42, 0x6f, 0x74, 0x45, 0x76, 0x65,
	0x6e, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x17, 0x0a, 0x07, 0x73, 0x74, 0x75,
	0x62, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x74, 0x75, 0x62,
	0x49, 0x64, 0x12, 0x1d, 0x0a, 0x0a, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x49,
	0x64, 0x12, 0x1d, 0x0a, 0x0a, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x54, 0x79, 0x70, 0x65,
	0x12, 0x1f, 0x0a, 0x0b, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x5f, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x56, 0x61, 0x6c, 0x75,
	0x65, 0x22, 0x26, 0x0a, 0x14, 0x50, 0x75, 0x73, 0x68, 0x42, 0x6f, 0x74, 0x45, 0x76, 0x65, 0x6e,
	0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x6f, 0x6b, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x02, 0x6f, 0x6b, 0x32, 0x80, 0x03, 0x0a, 0x0a, 0x42, 0x6f,
	0x74, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x48, 0x0a, 0x0d, 0x53, 0x74, 0x61, 0x72,
	0x74, 0x42, 0x6f, 0x74, 0x53, 0x65, 0x72, 0x76, 0x65, 0x12, 0x19, 0x2e, 0x62, 0x6f, 0x74, 0x2e,
	0x53, 0x74, 0x61, 0x72, 0x74, 0x42, 0x6f, 0x74, 0x53, 0x65, 0x72, 0x76, 0x65, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x1a, 0x1a, 0x2e, 0x62, 0x6f, 0x74, 0x2e, 0x53, 0x74, 0x61, 0x72, 0x74,
	0x42, 0x6f, 0x74, 0x53, 0x65, 0x72, 0x76, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x22, 0x00, 0x12, 0x54, 0x0a, 0x11, 0x42, 0x6f, 0x74, 0x53, 0x65, 0x72, 0x76, 0x65, 0x4b, 0x65,
	0x65, 0x70, 0x41, 0x6c, 0x69, 0x76, 0x65, 0x12, 0x1d, 0x2e, 0x62, 0x6f, 0x74, 0x2e, 0x42, 0x6f,
	0x74, 0x53, 0x65, 0x72, 0x76, 0x65, 0x4b, 0x65, 0x65, 0x70, 0x41, 0x6c, 0x69, 0x76, 0x65, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1e, 0x2e, 0x62, 0x6f, 0x74, 0x2e, 0x42, 0x6f, 0x74,
	0x53, 0x65, 0x72, 0x76, 0x65, 0x4b, 0x65, 0x65, 0x70, 0x41, 0x6c, 0x69, 0x76, 0x65, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x3f, 0x0a, 0x0a, 0x50, 0x6f, 0x70, 0x42,
	0x6f, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x16, 0x2e, 0x62, 0x6f, 0x74, 0x2e, 0x50, 0x6f, 0x70,
	0x42, 0x6f, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x17,
	0x2e, 0x62, 0x6f, 0x74, 0x2e, 0x50, 0x6f, 0x70, 0x42, 0x6f, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x4a, 0x0a, 0x0d, 0x50, 0x75, 0x73,
	0x68, 0x42, 0x6f, 0x74, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x12, 0x1a, 0x2e, 0x62, 0x6f, 0x74,
	0x2e, 0x50, 0x75, 0x73, 0x68, 0x42, 0x6f, 0x74, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1b, 0x2e, 0x62, 0x6f, 0x74, 0x2e, 0x50, 0x75, 0x73,
	0x68, 0x42, 0x6f, 0x74, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x45, 0x0a, 0x0c, 0x50, 0x75, 0x73, 0x68, 0x42, 0x6f, 0x74,
	0x45, 0x76, 0x65, 0x6e, 0x74, 0x12, 0x18, 0x2e, 0x62, 0x6f, 0x74, 0x2e, 0x50, 0x75, 0x73, 0x68,
	0x42, 0x6f, 0x74, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a,
	0x19, 0x2e, 0x62, 0x6f, 0x74, 0x2e, 0x50, 0x75, 0x73, 0x68, 0x42, 0x6f, 0x74, 0x45, 0x76, 0x65,
	0x6e, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x23, 0x5a, 0x21,
	0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x62, 0x65, 0x61, 0x6d, 0x2d,
	0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2f, 0x62, 0x65, 0x74, 0x61, 0x39, 0x2f, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_bot_proto_rawDescOnce sync.Once
	file_bot_proto_rawDescData = file_bot_proto_rawDesc
)

func file_bot_proto_rawDescGZIP() []byte {
	file_bot_proto_rawDescOnce.Do(func() {
		file_bot_proto_rawDescData = protoimpl.X.CompressGZIP(file_bot_proto_rawDescData)
	})
	return file_bot_proto_rawDescData
}

var file_bot_proto_msgTypes = make([]protoimpl.MessageInfo, 16)
var file_bot_proto_goTypes = []interface{}{
	(*StartBotServeRequest)(nil),             // 0: bot.StartBotServeRequest
	(*StartBotServeResponse)(nil),            // 1: bot.StartBotServeResponse
	(*BotServeKeepAliveRequest)(nil),         // 2: bot.BotServeKeepAliveRequest
	(*BotServeKeepAliveResponse)(nil),        // 3: bot.BotServeKeepAliveResponse
	(*PopBotTaskRequest)(nil),                // 4: bot.PopBotTaskRequest
	(*PopBotTaskResponse)(nil),               // 5: bot.PopBotTaskResponse
	(*MarkerField)(nil),                      // 6: bot.MarkerField
	(*Marker)(nil),                           // 7: bot.Marker
	(*PushBotMarkersRequest)(nil),            // 8: bot.PushBotMarkersRequest
	(*PushBotMarkersResponse)(nil),           // 9: bot.PushBotMarkersResponse
	(*PushBotEventRequest)(nil),              // 10: bot.PushBotEventRequest
	(*PushBotEventResponse)(nil),             // 11: bot.PushBotEventResponse
	nil,                                      // 12: bot.PopBotTaskResponse.MarkersEntry
	(*PopBotTaskResponse_MarkerList)(nil),    // 13: bot.PopBotTaskResponse.MarkerList
	nil,                                      // 14: bot.PushBotMarkersRequest.MarkersEntry
	(*PushBotMarkersRequest_MarkerList)(nil), // 15: bot.PushBotMarkersRequest.MarkerList
}
var file_bot_proto_depIdxs = []int32{
	12, // 0: bot.PopBotTaskResponse.markers:type_name -> bot.PopBotTaskResponse.MarkersEntry
	6,  // 1: bot.Marker.fields:type_name -> bot.MarkerField
	14, // 2: bot.PushBotMarkersRequest.markers:type_name -> bot.PushBotMarkersRequest.MarkersEntry
	13, // 3: bot.PopBotTaskResponse.MarkersEntry.value:type_name -> bot.PopBotTaskResponse.MarkerList
	7,  // 4: bot.PopBotTaskResponse.MarkerList.markers:type_name -> bot.Marker
	15, // 5: bot.PushBotMarkersRequest.MarkersEntry.value:type_name -> bot.PushBotMarkersRequest.MarkerList
	7,  // 6: bot.PushBotMarkersRequest.MarkerList.markers:type_name -> bot.Marker
	0,  // 7: bot.BotService.StartBotServe:input_type -> bot.StartBotServeRequest
	2,  // 8: bot.BotService.BotServeKeepAlive:input_type -> bot.BotServeKeepAliveRequest
	4,  // 9: bot.BotService.PopBotTask:input_type -> bot.PopBotTaskRequest
	8,  // 10: bot.BotService.PushBotMarker:input_type -> bot.PushBotMarkersRequest
	10, // 11: bot.BotService.PushBotEvent:input_type -> bot.PushBotEventRequest
	1,  // 12: bot.BotService.StartBotServe:output_type -> bot.StartBotServeResponse
	3,  // 13: bot.BotService.BotServeKeepAlive:output_type -> bot.BotServeKeepAliveResponse
	5,  // 14: bot.BotService.PopBotTask:output_type -> bot.PopBotTaskResponse
	9,  // 15: bot.BotService.PushBotMarker:output_type -> bot.PushBotMarkersResponse
	11, // 16: bot.BotService.PushBotEvent:output_type -> bot.PushBotEventResponse
	12, // [12:17] is the sub-list for method output_type
	7,  // [7:12] is the sub-list for method input_type
	7,  // [7:7] is the sub-list for extension type_name
	7,  // [7:7] is the sub-list for extension extendee
	0,  // [0:7] is the sub-list for field type_name
}

func init() { file_bot_proto_init() }
func file_bot_proto_init() {
	if File_bot_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_bot_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StartBotServeRequest); i {
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
		file_bot_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StartBotServeResponse); i {
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
		file_bot_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BotServeKeepAliveRequest); i {
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
		file_bot_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BotServeKeepAliveResponse); i {
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
		file_bot_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PopBotTaskRequest); i {
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
		file_bot_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PopBotTaskResponse); i {
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
		file_bot_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MarkerField); i {
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
		file_bot_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Marker); i {
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
		file_bot_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PushBotMarkersRequest); i {
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
		file_bot_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PushBotMarkersResponse); i {
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
		file_bot_proto_msgTypes[10].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PushBotEventRequest); i {
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
		file_bot_proto_msgTypes[11].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PushBotEventResponse); i {
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
		file_bot_proto_msgTypes[13].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PopBotTaskResponse_MarkerList); i {
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
		file_bot_proto_msgTypes[15].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PushBotMarkersRequest_MarkerList); i {
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
			RawDescriptor: file_bot_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   16,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_bot_proto_goTypes,
		DependencyIndexes: file_bot_proto_depIdxs,
		MessageInfos:      file_bot_proto_msgTypes,
	}.Build()
	File_bot_proto = out.File
	file_bot_proto_rawDesc = nil
	file_bot_proto_goTypes = nil
	file_bot_proto_depIdxs = nil
}
