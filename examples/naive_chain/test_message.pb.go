// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.5
// 	protoc        v5.29.3
// source: examples/naive_chain/test_message.proto

package naive

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type FwdMessage struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Sender        uint64                 `protobuf:"varint,1,opt,name=sender,proto3" json:"sender,omitempty"`
	Payload       []byte                 `protobuf:"bytes,2,opt,name=payload,proto3" json:"payload,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *FwdMessage) Reset() {
	*x = FwdMessage{}
	mi := &file_examples_naive_chain_test_message_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FwdMessage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FwdMessage) ProtoMessage() {}

func (x *FwdMessage) ProtoReflect() protoreflect.Message {
	mi := &file_examples_naive_chain_test_message_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FwdMessage.ProtoReflect.Descriptor instead.
func (*FwdMessage) Descriptor() ([]byte, []int) {
	return file_examples_naive_chain_test_message_proto_rawDescGZIP(), []int{0}
}

func (x *FwdMessage) GetSender() uint64 {
	if x != nil {
		return x.Sender
	}
	return 0
}

func (x *FwdMessage) GetPayload() []byte {
	if x != nil {
		return x.Payload
	}
	return nil
}

var File_examples_naive_chain_test_message_proto protoreflect.FileDescriptor

var file_examples_naive_chain_test_message_proto_rawDesc = string([]byte{
	0x0a, 0x27, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x73, 0x2f, 0x6e, 0x61, 0x69, 0x76, 0x65,
	0x5f, 0x63, 0x68, 0x61, 0x69, 0x6e, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x6d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x05, 0x6e, 0x61, 0x69, 0x76, 0x65,
	0x22, 0x3e, 0x0a, 0x0a, 0x46, 0x77, 0x64, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x16,
	0x0a, 0x06, 0x73, 0x65, 0x6e, 0x64, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x06,
	0x73, 0x65, 0x6e, 0x64, 0x65, 0x72, 0x12, 0x18, 0x0a, 0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61,
	0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64,
	0x42, 0x2c, 0x5a, 0x2a, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x68,
	0x79, 0x70, 0x65, 0x72, 0x6c, 0x65, 0x64, 0x67, 0x65, 0x72, 0x2d, 0x6c, 0x61, 0x62, 0x73, 0x2f,
	0x53, 0x6d, 0x61, 0x72, 0x74, 0x42, 0x46, 0x54, 0x2f, 0x6e, 0x61, 0x69, 0x76, 0x65, 0x62, 0x06,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
})

var (
	file_examples_naive_chain_test_message_proto_rawDescOnce sync.Once
	file_examples_naive_chain_test_message_proto_rawDescData []byte
)

func file_examples_naive_chain_test_message_proto_rawDescGZIP() []byte {
	file_examples_naive_chain_test_message_proto_rawDescOnce.Do(func() {
		file_examples_naive_chain_test_message_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_examples_naive_chain_test_message_proto_rawDesc), len(file_examples_naive_chain_test_message_proto_rawDesc)))
	})
	return file_examples_naive_chain_test_message_proto_rawDescData
}

var file_examples_naive_chain_test_message_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_examples_naive_chain_test_message_proto_goTypes = []any{
	(*FwdMessage)(nil), // 0: naive.FwdMessage
}
var file_examples_naive_chain_test_message_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_examples_naive_chain_test_message_proto_init() }
func file_examples_naive_chain_test_message_proto_init() {
	if File_examples_naive_chain_test_message_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_examples_naive_chain_test_message_proto_rawDesc), len(file_examples_naive_chain_test_message_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_examples_naive_chain_test_message_proto_goTypes,
		DependencyIndexes: file_examples_naive_chain_test_message_proto_depIdxs,
		MessageInfos:      file_examples_naive_chain_test_message_proto_msgTypes,
	}.Build()
	File_examples_naive_chain_test_message_proto = out.File
	file_examples_naive_chain_test_message_proto_goTypes = nil
	file_examples_naive_chain_test_message_proto_depIdxs = nil
}
