### Generated by rprotoc. DO NOT EDIT!
### <proto file: log.proto>
# message LogEvent {
#     enum Severity {
#             INFO = 0;
#                     WARNING = 1;
#                             CRITICAL = 2;
#                                 }
#                                     optional bytes data = 1;
#                                         optional Severity severity = 2 [default = INFO]; // only uploaded if collector gets it from syslog/eventlog/etc.
#                                             optional fixed32 time = 3; // time taken from syslog/eventlog if given, otherwise current system time.
#                                                 optional string name = 4; // the location this came from, can be empty if none given.
#                                                     optional string classifier = 5; // facility if from a SYSLOG log.
#                                                     }

require 'protobuf/message/message'
require 'protobuf/message/enum'
require 'protobuf/message/service'
require 'protobuf/message/extend'

class LogEvent < ::Protobuf::Message
  defined_in __FILE__
  class Severity < ::Protobuf::Enum
    defined_in __FILE__
    INFO = value(:INFO, 0)
    WARNING = value(:WARNING, 1)
    CRITICAL = value(:CRITICAL, 2)
  end
  optional :bytes, :data, 1
  optional :Severity, :severity, 2, :default => :INFO
  optional :fixed32, :time, 3
  optional :string, :name, 4
  optional :string, :classifier, 5
end
