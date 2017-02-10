#!/usr/bin/env ruby

require "gi"

Arrow = GI.load("Arrow")
ArrowIO = GI.load("ArrowIO")
ArrowIPC = GI.load("ArrowIPC")

module Arrow
  class Array
    def [](i)
      get_value(i)
    end

    include Enumerable
    def each
      length.times do |i|
        yield(self[i])
      end
    end
  end
end

file = ArrowIO::MemoryMappedFile.open("/tmp/xxx", :read)
reader = ArrowIPC::FileReader.open(file)
p reader.schema.fields.collect(&:name)
record_batch = reader.get_record_batch(0)
record_batch.n_rows.times do |i|
  p record_batch.columns.collect {|column| column[i]}
end
