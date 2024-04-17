# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: raft.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\nraft.proto\x12\tmapreduce\"]\n\x0bMapperInput\x12\x12\n\nstartIndex\x18\x01 \x01(\t\x12\x10\n\x08\x65ndIndex\x18\x02 \x01(\t\x12\x14\n\x0coldCentroids\x18\x03 \x01(\t\x12\x12\n\nnumReducer\x18\x04 \x01(\t\"\x1f\n\x0cMapperOutput\x12\x0f\n\x07success\x18\x01 \x01(\x08\"5\n\rReduceRequest\x12\x11\n\treducerId\x18\x01 \x01(\t\x12\x11\n\tnumMapper\x18\x02 \x01(\t\"*\n\x0eReduceResponse\x12\x18\n\x10updated_centroid\x18\x01 \x01(\t\"0\n\x1bRequestPartitionDataRequest\x12\x11\n\treducerID\x18\x01 \x01(\t\",\n\x1cRequestPartitionDataResponse\x12\x0c\n\x04\x64\x61ta\x18\x01 \x01(\t2\xef\x01\n\tMapReduce\x12\x38\n\x03map\x12\x16.mapreduce.MapperInput\x1a\x17.mapreduce.MapperOutput\"\x00\x12?\n\x06reduce\x12\x18.mapreduce.ReduceRequest\x1a\x19.mapreduce.ReduceResponse\"\x00\x12g\n\x14RequestPartitionData\x12&.mapreduce.RequestPartitionDataRequest\x1a\'.mapreduce.RequestPartitionDataResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'raft_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_MAPPERINPUT']._serialized_start=25
  _globals['_MAPPERINPUT']._serialized_end=118
  _globals['_MAPPEROUTPUT']._serialized_start=120
  _globals['_MAPPEROUTPUT']._serialized_end=151
  _globals['_REDUCEREQUEST']._serialized_start=153
  _globals['_REDUCEREQUEST']._serialized_end=206
  _globals['_REDUCERESPONSE']._serialized_start=208
  _globals['_REDUCERESPONSE']._serialized_end=250
  _globals['_REQUESTPARTITIONDATAREQUEST']._serialized_start=252
  _globals['_REQUESTPARTITIONDATAREQUEST']._serialized_end=300
  _globals['_REQUESTPARTITIONDATARESPONSE']._serialized_start=302
  _globals['_REQUESTPARTITIONDATARESPONSE']._serialized_end=346
  _globals['_MAPREDUCE']._serialized_start=349
  _globals['_MAPREDUCE']._serialized_end=588
# @@protoc_insertion_point(module_scope)
