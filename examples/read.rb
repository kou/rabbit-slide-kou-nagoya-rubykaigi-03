#!/usr/bin/env ruby

require "gi"

Arrow = GI.load("Arrow")
ArrowIO = GI.load("ArrowIO")
ArrowIPC = GI.load("ArrowIPC")

file = ArrowIO::MemoryMappedFile.open("/tmp/xxx", :read)
reader = ArrowIPC::FileReader.open(file)
p reader.schema.fields.collect(&:name)
record_batch = reader.get_record_batch(0)
p record_batch.columns.collect(&:length)
p record_batch.columns.collect {|column| column.get_value(0)}
