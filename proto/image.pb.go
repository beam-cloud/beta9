// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v4.25.1
// source: image.proto

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

type BuildStep struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Type    string `protobuf:"bytes,1,opt,name=type,proto3" json:"type,omitempty"`
	Command string `protobuf:"bytes,2,opt,name=command,proto3" json:"command,omitempty"`
}

func (x *BuildStep) Reset() {
	*x = BuildStep{}
	if protoimpl.UnsafeEnabled {
		mi := &file_image_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BuildStep) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BuildStep) ProtoMessage() {}

func (x *BuildStep) ProtoReflect() protoreflect.Message {
	mi := &file_image_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BuildStep.ProtoReflect.Descriptor instead.
func (*BuildStep) Descriptor() ([]byte, []int) {
	return file_image_proto_rawDescGZIP(), []int{0}
}

func (x *BuildStep) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

func (x *BuildStep) GetCommand() string {
	if x != nil {
		return x.Command
	}
	return ""
}

type VerifyImageBuildRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	PythonVersion    string       `protobuf:"bytes,1,opt,name=python_version,json=pythonVersion,proto3" json:"python_version,omitempty"`
	PythonPackages   []string     `protobuf:"bytes,2,rep,name=python_packages,json=pythonPackages,proto3" json:"python_packages,omitempty"`
	Commands         []string     `protobuf:"bytes,3,rep,name=commands,proto3" json:"commands,omitempty"`
	ForceRebuild     bool         `protobuf:"varint,4,opt,name=force_rebuild,json=forceRebuild,proto3" json:"force_rebuild,omitempty"`
	ExistingImageUri string       `protobuf:"bytes,5,opt,name=existing_image_uri,json=existingImageUri,proto3" json:"existing_image_uri,omitempty"`
	BuildSteps       []*BuildStep `protobuf:"bytes,6,rep,name=build_steps,json=buildSteps,proto3" json:"build_steps,omitempty"`
	EnvVars          []string     `protobuf:"bytes,7,rep,name=env_vars,json=envVars,proto3" json:"env_vars,omitempty"`
	Dockerfile       string       `protobuf:"bytes,8,opt,name=dockerfile,proto3" json:"dockerfile,omitempty"`
	BuildCtxObject   string       `protobuf:"bytes,9,opt,name=build_ctx_object,json=buildCtxObject,proto3" json:"build_ctx_object,omitempty"`
	Secrets          []string     `protobuf:"bytes,10,rep,name=secrets,proto3" json:"secrets,omitempty"`
	Gpu              string       `protobuf:"bytes,11,opt,name=gpu,proto3" json:"gpu,omitempty"`
	IgnorePython     bool         `protobuf:"varint,12,opt,name=ignore_python,json=ignorePython,proto3" json:"ignore_python,omitempty"`
	SnapshotId       *string      `protobuf:"bytes,13,opt,name=snapshot_id,json=snapshotId,proto3,oneof" json:"snapshot_id,omitempty"`
}

func (x *VerifyImageBuildRequest) Reset() {
	*x = VerifyImageBuildRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_image_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *VerifyImageBuildRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*VerifyImageBuildRequest) ProtoMessage() {}

func (x *VerifyImageBuildRequest) ProtoReflect() protoreflect.Message {
	mi := &file_image_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use VerifyImageBuildRequest.ProtoReflect.Descriptor instead.
func (*VerifyImageBuildRequest) Descriptor() ([]byte, []int) {
	return file_image_proto_rawDescGZIP(), []int{1}
}

func (x *VerifyImageBuildRequest) GetPythonVersion() string {
	if x != nil {
		return x.PythonVersion
	}
	return ""
}

func (x *VerifyImageBuildRequest) GetPythonPackages() []string {
	if x != nil {
		return x.PythonPackages
	}
	return nil
}

func (x *VerifyImageBuildRequest) GetCommands() []string {
	if x != nil {
		return x.Commands
	}
	return nil
}

func (x *VerifyImageBuildRequest) GetForceRebuild() bool {
	if x != nil {
		return x.ForceRebuild
	}
	return false
}

func (x *VerifyImageBuildRequest) GetExistingImageUri() string {
	if x != nil {
		return x.ExistingImageUri
	}
	return ""
}

func (x *VerifyImageBuildRequest) GetBuildSteps() []*BuildStep {
	if x != nil {
		return x.BuildSteps
	}
	return nil
}

func (x *VerifyImageBuildRequest) GetEnvVars() []string {
	if x != nil {
		return x.EnvVars
	}
	return nil
}

func (x *VerifyImageBuildRequest) GetDockerfile() string {
	if x != nil {
		return x.Dockerfile
	}
	return ""
}

func (x *VerifyImageBuildRequest) GetBuildCtxObject() string {
	if x != nil {
		return x.BuildCtxObject
	}
	return ""
}

func (x *VerifyImageBuildRequest) GetSecrets() []string {
	if x != nil {
		return x.Secrets
	}
	return nil
}

func (x *VerifyImageBuildRequest) GetGpu() string {
	if x != nil {
		return x.Gpu
	}
	return ""
}

func (x *VerifyImageBuildRequest) GetIgnorePython() bool {
	if x != nil {
		return x.IgnorePython
	}
	return false
}

func (x *VerifyImageBuildRequest) GetSnapshotId() string {
	if x != nil && x.SnapshotId != nil {
		return *x.SnapshotId
	}
	return ""
}

type VerifyImageBuildResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ImageId string `protobuf:"bytes,1,opt,name=image_id,json=imageId,proto3" json:"image_id,omitempty"`
	Valid   bool   `protobuf:"varint,2,opt,name=valid,proto3" json:"valid,omitempty"`
	Exists  bool   `protobuf:"varint,3,opt,name=exists,proto3" json:"exists,omitempty"`
}

func (x *VerifyImageBuildResponse) Reset() {
	*x = VerifyImageBuildResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_image_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *VerifyImageBuildResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*VerifyImageBuildResponse) ProtoMessage() {}

func (x *VerifyImageBuildResponse) ProtoReflect() protoreflect.Message {
	mi := &file_image_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use VerifyImageBuildResponse.ProtoReflect.Descriptor instead.
func (*VerifyImageBuildResponse) Descriptor() ([]byte, []int) {
	return file_image_proto_rawDescGZIP(), []int{2}
}

func (x *VerifyImageBuildResponse) GetImageId() string {
	if x != nil {
		return x.ImageId
	}
	return ""
}

func (x *VerifyImageBuildResponse) GetValid() bool {
	if x != nil {
		return x.Valid
	}
	return false
}

func (x *VerifyImageBuildResponse) GetExists() bool {
	if x != nil {
		return x.Exists
	}
	return false
}

type BuildImageRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// These parameters are used for a "beta9" managed image
	PythonVersion  string   `protobuf:"bytes,1,opt,name=python_version,json=pythonVersion,proto3" json:"python_version,omitempty"`
	PythonPackages []string `protobuf:"bytes,2,rep,name=python_packages,json=pythonPackages,proto3" json:"python_packages,omitempty"`
	Commands       []string `protobuf:"bytes,3,rep,name=commands,proto3" json:"commands,omitempty"`
	// These parameters are used for an existing image
	ExistingImageUri   string            `protobuf:"bytes,4,opt,name=existing_image_uri,json=existingImageUri,proto3" json:"existing_image_uri,omitempty"` // URI for an existing image in the format
	ExistingImageCreds map[string]string `protobuf:"bytes,5,rep,name=existing_image_creds,json=existingImageCreds,proto3" json:"existing_image_creds,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	BuildSteps         []*BuildStep      `protobuf:"bytes,6,rep,name=build_steps,json=buildSteps,proto3" json:"build_steps,omitempty"`
	EnvVars            []string          `protobuf:"bytes,7,rep,name=env_vars,json=envVars,proto3" json:"env_vars,omitempty"`
	Dockerfile         string            `protobuf:"bytes,8,opt,name=dockerfile,proto3" json:"dockerfile,omitempty"`
	BuildCtxObject     string            `protobuf:"bytes,9,opt,name=build_ctx_object,json=buildCtxObject,proto3" json:"build_ctx_object,omitempty"`
	Secrets            []string          `protobuf:"bytes,10,rep,name=secrets,proto3" json:"secrets,omitempty"`
	Gpu                string            `protobuf:"bytes,11,opt,name=gpu,proto3" json:"gpu,omitempty"`
	IgnorePython       bool              `protobuf:"varint,12,opt,name=ignore_python,json=ignorePython,proto3" json:"ignore_python,omitempty"`
}

func (x *BuildImageRequest) Reset() {
	*x = BuildImageRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_image_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BuildImageRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BuildImageRequest) ProtoMessage() {}

func (x *BuildImageRequest) ProtoReflect() protoreflect.Message {
	mi := &file_image_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BuildImageRequest.ProtoReflect.Descriptor instead.
func (*BuildImageRequest) Descriptor() ([]byte, []int) {
	return file_image_proto_rawDescGZIP(), []int{3}
}

func (x *BuildImageRequest) GetPythonVersion() string {
	if x != nil {
		return x.PythonVersion
	}
	return ""
}

func (x *BuildImageRequest) GetPythonPackages() []string {
	if x != nil {
		return x.PythonPackages
	}
	return nil
}

func (x *BuildImageRequest) GetCommands() []string {
	if x != nil {
		return x.Commands
	}
	return nil
}

func (x *BuildImageRequest) GetExistingImageUri() string {
	if x != nil {
		return x.ExistingImageUri
	}
	return ""
}

func (x *BuildImageRequest) GetExistingImageCreds() map[string]string {
	if x != nil {
		return x.ExistingImageCreds
	}
	return nil
}

func (x *BuildImageRequest) GetBuildSteps() []*BuildStep {
	if x != nil {
		return x.BuildSteps
	}
	return nil
}

func (x *BuildImageRequest) GetEnvVars() []string {
	if x != nil {
		return x.EnvVars
	}
	return nil
}

func (x *BuildImageRequest) GetDockerfile() string {
	if x != nil {
		return x.Dockerfile
	}
	return ""
}

func (x *BuildImageRequest) GetBuildCtxObject() string {
	if x != nil {
		return x.BuildCtxObject
	}
	return ""
}

func (x *BuildImageRequest) GetSecrets() []string {
	if x != nil {
		return x.Secrets
	}
	return nil
}

func (x *BuildImageRequest) GetGpu() string {
	if x != nil {
		return x.Gpu
	}
	return ""
}

func (x *BuildImageRequest) GetIgnorePython() bool {
	if x != nil {
		return x.IgnorePython
	}
	return false
}

type BuildImageResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ImageId       string `protobuf:"bytes,1,opt,name=image_id,json=imageId,proto3" json:"image_id,omitempty"`
	Msg           string `protobuf:"bytes,2,opt,name=msg,proto3" json:"msg,omitempty"`
	Done          bool   `protobuf:"varint,3,opt,name=done,proto3" json:"done,omitempty"`
	Success       bool   `protobuf:"varint,4,opt,name=success,proto3" json:"success,omitempty"`
	PythonVersion string `protobuf:"bytes,5,opt,name=python_version,json=pythonVersion,proto3" json:"python_version,omitempty"`
	Warning       bool   `protobuf:"varint,6,opt,name=warning,proto3" json:"warning,omitempty"`
}

func (x *BuildImageResponse) Reset() {
	*x = BuildImageResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_image_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BuildImageResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BuildImageResponse) ProtoMessage() {}

func (x *BuildImageResponse) ProtoReflect() protoreflect.Message {
	mi := &file_image_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BuildImageResponse.ProtoReflect.Descriptor instead.
func (*BuildImageResponse) Descriptor() ([]byte, []int) {
	return file_image_proto_rawDescGZIP(), []int{4}
}

func (x *BuildImageResponse) GetImageId() string {
	if x != nil {
		return x.ImageId
	}
	return ""
}

func (x *BuildImageResponse) GetMsg() string {
	if x != nil {
		return x.Msg
	}
	return ""
}

func (x *BuildImageResponse) GetDone() bool {
	if x != nil {
		return x.Done
	}
	return false
}

func (x *BuildImageResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

func (x *BuildImageResponse) GetPythonVersion() string {
	if x != nil {
		return x.PythonVersion
	}
	return ""
}

func (x *BuildImageResponse) GetWarning() bool {
	if x != nil {
		return x.Warning
	}
	return false
}

var File_image_proto protoreflect.FileDescriptor

var file_image_proto_rawDesc = []byte{
	0x0a, 0x0b, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x05, 0x69,
	0x6d, 0x61, 0x67, 0x65, 0x22, 0x39, 0x0a, 0x09, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x53, 0x74, 0x65,
	0x70, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x04, 0x74, 0x79, 0x70, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x22,
	0xf7, 0x03, 0x0a, 0x17, 0x56, 0x65, 0x72, 0x69, 0x66, 0x79, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x42,
	0x75, 0x69, 0x6c, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x25, 0x0a, 0x0e, 0x70,
	0x79, 0x74, 0x68, 0x6f, 0x6e, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0d, 0x70, 0x79, 0x74, 0x68, 0x6f, 0x6e, 0x56, 0x65, 0x72, 0x73, 0x69,
	0x6f, 0x6e, 0x12, 0x27, 0x0a, 0x0f, 0x70, 0x79, 0x74, 0x68, 0x6f, 0x6e, 0x5f, 0x70, 0x61, 0x63,
	0x6b, 0x61, 0x67, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0e, 0x70, 0x79, 0x74,
	0x68, 0x6f, 0x6e, 0x50, 0x61, 0x63, 0x6b, 0x61, 0x67, 0x65, 0x73, 0x12, 0x1a, 0x0a, 0x08, 0x63,
	0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x09, 0x52, 0x08, 0x63,
	0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x73, 0x12, 0x23, 0x0a, 0x0d, 0x66, 0x6f, 0x72, 0x63, 0x65,
	0x5f, 0x72, 0x65, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0c,
	0x66, 0x6f, 0x72, 0x63, 0x65, 0x52, 0x65, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x12, 0x2c, 0x0a, 0x12,
	0x65, 0x78, 0x69, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x5f, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x5f, 0x75,
	0x72, 0x69, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x10, 0x65, 0x78, 0x69, 0x73, 0x74, 0x69,
	0x6e, 0x67, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x55, 0x72, 0x69, 0x12, 0x31, 0x0a, 0x0b, 0x62, 0x75,
	0x69, 0x6c, 0x64, 0x5f, 0x73, 0x74, 0x65, 0x70, 0x73, 0x18, 0x06, 0x20, 0x03, 0x28, 0x0b, 0x32,
	0x10, 0x2e, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x2e, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x53, 0x74, 0x65,
	0x70, 0x52, 0x0a, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x53, 0x74, 0x65, 0x70, 0x73, 0x12, 0x19, 0x0a,
	0x08, 0x65, 0x6e, 0x76, 0x5f, 0x76, 0x61, 0x72, 0x73, 0x18, 0x07, 0x20, 0x03, 0x28, 0x09, 0x52,
	0x07, 0x65, 0x6e, 0x76, 0x56, 0x61, 0x72, 0x73, 0x12, 0x1e, 0x0a, 0x0a, 0x64, 0x6f, 0x63, 0x6b,
	0x65, 0x72, 0x66, 0x69, 0x6c, 0x65, 0x18, 0x08, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x64, 0x6f,
	0x63, 0x6b, 0x65, 0x72, 0x66, 0x69, 0x6c, 0x65, 0x12, 0x28, 0x0a, 0x10, 0x62, 0x75, 0x69, 0x6c,
	0x64, 0x5f, 0x63, 0x74, 0x78, 0x5f, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x18, 0x09, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x43, 0x74, 0x78, 0x4f, 0x62, 0x6a, 0x65,
	0x63, 0x74, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x73, 0x18, 0x0a, 0x20,
	0x03, 0x28, 0x09, 0x52, 0x07, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x73, 0x12, 0x10, 0x0a, 0x03,
	0x67, 0x70, 0x75, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x67, 0x70, 0x75, 0x12, 0x23,
	0x0a, 0x0d, 0x69, 0x67, 0x6e, 0x6f, 0x72, 0x65, 0x5f, 0x70, 0x79, 0x74, 0x68, 0x6f, 0x6e, 0x18,
	0x0c, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0c, 0x69, 0x67, 0x6e, 0x6f, 0x72, 0x65, 0x50, 0x79, 0x74,
	0x68, 0x6f, 0x6e, 0x12, 0x24, 0x0a, 0x0b, 0x73, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x5f,
	0x69, 0x64, 0x18, 0x0d, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x0a, 0x73, 0x6e, 0x61, 0x70,
	0x73, 0x68, 0x6f, 0x74, 0x49, 0x64, 0x88, 0x01, 0x01, 0x42, 0x0e, 0x0a, 0x0c, 0x5f, 0x73, 0x6e,
	0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x5f, 0x69, 0x64, 0x22, 0x63, 0x0a, 0x18, 0x56, 0x65, 0x72,
	0x69, 0x66, 0x79, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x19, 0x0a, 0x08, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x5f, 0x69,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x49, 0x64,
	0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52,
	0x05, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x12, 0x16, 0x0a, 0x06, 0x65, 0x78, 0x69, 0x73, 0x74, 0x73,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x08, 0x52, 0x06, 0x65, 0x78, 0x69, 0x73, 0x74, 0x73, 0x22, 0xc1,
	0x04, 0x0a, 0x11, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x25, 0x0a, 0x0e, 0x70, 0x79, 0x74, 0x68, 0x6f, 0x6e, 0x5f, 0x76,
	0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x70, 0x79,
	0x74, 0x68, 0x6f, 0x6e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x27, 0x0a, 0x0f, 0x70,
	0x79, 0x74, 0x68, 0x6f, 0x6e, 0x5f, 0x70, 0x61, 0x63, 0x6b, 0x61, 0x67, 0x65, 0x73, 0x18, 0x02,
	0x20, 0x03, 0x28, 0x09, 0x52, 0x0e, 0x70, 0x79, 0x74, 0x68, 0x6f, 0x6e, 0x50, 0x61, 0x63, 0x6b,
	0x61, 0x67, 0x65, 0x73, 0x12, 0x1a, 0x0a, 0x08, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x73,
	0x18, 0x03, 0x20, 0x03, 0x28, 0x09, 0x52, 0x08, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x73,
	0x12, 0x2c, 0x0a, 0x12, 0x65, 0x78, 0x69, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x5f, 0x69, 0x6d, 0x61,
	0x67, 0x65, 0x5f, 0x75, 0x72, 0x69, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x10, 0x65, 0x78,
	0x69, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x55, 0x72, 0x69, 0x12, 0x62,
	0x0a, 0x14, 0x65, 0x78, 0x69, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x5f, 0x69, 0x6d, 0x61, 0x67, 0x65,
	0x5f, 0x63, 0x72, 0x65, 0x64, 0x73, 0x18, 0x05, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x30, 0x2e, 0x69,
	0x6d, 0x61, 0x67, 0x65, 0x2e, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x45, 0x78, 0x69, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x49,
	0x6d, 0x61, 0x67, 0x65, 0x43, 0x72, 0x65, 0x64, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x12,
	0x65, 0x78, 0x69, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x43, 0x72, 0x65,
	0x64, 0x73, 0x12, 0x31, 0x0a, 0x0b, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x5f, 0x73, 0x74, 0x65, 0x70,
	0x73, 0x18, 0x06, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x2e,
	0x42, 0x75, 0x69, 0x6c, 0x64, 0x53, 0x74, 0x65, 0x70, 0x52, 0x0a, 0x62, 0x75, 0x69, 0x6c, 0x64,
	0x53, 0x74, 0x65, 0x70, 0x73, 0x12, 0x19, 0x0a, 0x08, 0x65, 0x6e, 0x76, 0x5f, 0x76, 0x61, 0x72,
	0x73, 0x18, 0x07, 0x20, 0x03, 0x28, 0x09, 0x52, 0x07, 0x65, 0x6e, 0x76, 0x56, 0x61, 0x72, 0x73,
	0x12, 0x1e, 0x0a, 0x0a, 0x64, 0x6f, 0x63, 0x6b, 0x65, 0x72, 0x66, 0x69, 0x6c, 0x65, 0x18, 0x08,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x64, 0x6f, 0x63, 0x6b, 0x65, 0x72, 0x66, 0x69, 0x6c, 0x65,
	0x12, 0x28, 0x0a, 0x10, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x5f, 0x63, 0x74, 0x78, 0x5f, 0x6f, 0x62,
	0x6a, 0x65, 0x63, 0x74, 0x18, 0x09, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0e, 0x62, 0x75, 0x69, 0x6c,
	0x64, 0x43, 0x74, 0x78, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x65,
	0x63, 0x72, 0x65, 0x74, 0x73, 0x18, 0x0a, 0x20, 0x03, 0x28, 0x09, 0x52, 0x07, 0x73, 0x65, 0x63,
	0x72, 0x65, 0x74, 0x73, 0x12, 0x10, 0x0a, 0x03, 0x67, 0x70, 0x75, 0x18, 0x0b, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x67, 0x70, 0x75, 0x12, 0x23, 0x0a, 0x0d, 0x69, 0x67, 0x6e, 0x6f, 0x72, 0x65,
	0x5f, 0x70, 0x79, 0x74, 0x68, 0x6f, 0x6e, 0x18, 0x0c, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0c, 0x69,
	0x67, 0x6e, 0x6f, 0x72, 0x65, 0x50, 0x79, 0x74, 0x68, 0x6f, 0x6e, 0x1a, 0x45, 0x0a, 0x17, 0x45,
	0x78, 0x69, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x43, 0x72, 0x65, 0x64,
	0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02,
	0x38, 0x01, 0x22, 0xb0, 0x01, 0x0a, 0x12, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x6d, 0x61, 0x67,
	0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x19, 0x0a, 0x08, 0x69, 0x6d, 0x61,
	0x67, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x69, 0x6d, 0x61,
	0x67, 0x65, 0x49, 0x64, 0x12, 0x10, 0x0a, 0x03, 0x6d, 0x73, 0x67, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6d, 0x73, 0x67, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x6f, 0x6e, 0x65, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x08, 0x52, 0x04, 0x64, 0x6f, 0x6e, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x75,
	0x63, 0x63, 0x65, 0x73, 0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x73, 0x75, 0x63,
	0x63, 0x65, 0x73, 0x73, 0x12, 0x25, 0x0a, 0x0e, 0x70, 0x79, 0x74, 0x68, 0x6f, 0x6e, 0x5f, 0x76,
	0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x70, 0x79,
	0x74, 0x68, 0x6f, 0x6e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x18, 0x0a, 0x07, 0x77,
	0x61, 0x72, 0x6e, 0x69, 0x6e, 0x67, 0x18, 0x06, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x77, 0x61,
	0x72, 0x6e, 0x69, 0x6e, 0x67, 0x32, 0xac, 0x01, 0x0a, 0x0c, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x53,
	0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x55, 0x0a, 0x10, 0x56, 0x65, 0x72, 0x69, 0x66, 0x79,
	0x49, 0x6d, 0x61, 0x67, 0x65, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x12, 0x1e, 0x2e, 0x69, 0x6d, 0x61,
	0x67, 0x65, 0x2e, 0x56, 0x65, 0x72, 0x69, 0x66, 0x79, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x42, 0x75,
	0x69, 0x6c, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1f, 0x2e, 0x69, 0x6d, 0x61,
	0x67, 0x65, 0x2e, 0x56, 0x65, 0x72, 0x69, 0x66, 0x79, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x42, 0x75,
	0x69, 0x6c, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x45, 0x0a,
	0x0a, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x12, 0x18, 0x2e, 0x69, 0x6d,
	0x61, 0x67, 0x65, 0x2e, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x19, 0x2e, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x2e, 0x42, 0x75,
	0x69, 0x6c, 0x64, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x22, 0x00, 0x30, 0x01, 0x42, 0x23, 0x5a, 0x21, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63,
	0x6f, 0x6d, 0x2f, 0x62, 0x65, 0x61, 0x6d, 0x2d, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2f, 0x62, 0x65,
	0x74, 0x61, 0x39, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_image_proto_rawDescOnce sync.Once
	file_image_proto_rawDescData = file_image_proto_rawDesc
)

func file_image_proto_rawDescGZIP() []byte {
	file_image_proto_rawDescOnce.Do(func() {
		file_image_proto_rawDescData = protoimpl.X.CompressGZIP(file_image_proto_rawDescData)
	})
	return file_image_proto_rawDescData
}

var file_image_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_image_proto_goTypes = []interface{}{
	(*BuildStep)(nil),                // 0: image.BuildStep
	(*VerifyImageBuildRequest)(nil),  // 1: image.VerifyImageBuildRequest
	(*VerifyImageBuildResponse)(nil), // 2: image.VerifyImageBuildResponse
	(*BuildImageRequest)(nil),        // 3: image.BuildImageRequest
	(*BuildImageResponse)(nil),       // 4: image.BuildImageResponse
	nil,                              // 5: image.BuildImageRequest.ExistingImageCredsEntry
}
var file_image_proto_depIdxs = []int32{
	0, // 0: image.VerifyImageBuildRequest.build_steps:type_name -> image.BuildStep
	5, // 1: image.BuildImageRequest.existing_image_creds:type_name -> image.BuildImageRequest.ExistingImageCredsEntry
	0, // 2: image.BuildImageRequest.build_steps:type_name -> image.BuildStep
	1, // 3: image.ImageService.VerifyImageBuild:input_type -> image.VerifyImageBuildRequest
	3, // 4: image.ImageService.BuildImage:input_type -> image.BuildImageRequest
	2, // 5: image.ImageService.VerifyImageBuild:output_type -> image.VerifyImageBuildResponse
	4, // 6: image.ImageService.BuildImage:output_type -> image.BuildImageResponse
	5, // [5:7] is the sub-list for method output_type
	3, // [3:5] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_image_proto_init() }
func file_image_proto_init() {
	if File_image_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_image_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BuildStep); i {
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
		file_image_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*VerifyImageBuildRequest); i {
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
		file_image_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*VerifyImageBuildResponse); i {
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
		file_image_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BuildImageRequest); i {
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
		file_image_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BuildImageResponse); i {
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
	file_image_proto_msgTypes[1].OneofWrappers = []interface{}{}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_image_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_image_proto_goTypes,
		DependencyIndexes: file_image_proto_depIdxs,
		MessageInfos:      file_image_proto_msgTypes,
	}.Build()
	File_image_proto = out.File
	file_image_proto_rawDesc = nil
	file_image_proto_goTypes = nil
	file_image_proto_depIdxs = nil
}
