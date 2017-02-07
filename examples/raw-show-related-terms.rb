#!/usr/bin/env ruby

require "groonga"
require "gi"

Arrow = GI.load("Arrow")
ArrowIO = GI.load("ArrowIO")
ArrowIPC = GI.load("ArrowIPC")

Groonga::Database.open(ARGV[0])
terms = Groonga["Words"]

input_stream = ArrowIO::MemoryMappedFile.open(ARGV[1], :read)
begin
  reader = ArrowIPC::StreamReader.open(input_stream)
  loop do
    record_batch = reader.next_record_batch
    break if record_batch.nil?
    columns = record_batch.columns
    related_terms = []
    record_batch.n_rows.times do |i|
      score = columns[1].get_value(i)
      next if score < 0.1
      term = Groonga::Record.new(terms, columns[0].get_value(i)).key
      related_terms << [term, score]
    end
    next if related_terms.size < 2
    p related_terms
  end
ensure
  input_stream.close
end
