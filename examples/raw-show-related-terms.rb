#!/usr/bin/env ruby

require "groonga"
require "gi"

db_path = ARGV[0]
output_path = ARGV[1]

Arrow = GI.load("Arrow")
ArrowIO = GI.load("ArrowIO")
ArrowIPC = GI.load("ArrowIPC")

Groonga::Database.open(db_path)
terms = Groonga["Words"]

input_stream = ArrowIO::MemoryMappedFile.open(output_path, :read)
begin
  reader = ArrowIPC::StreamReader.open(input_stream)
  loop do
    record_batch = reader.next_record_batch
    break if record_batch.nil?
    columns = record_batch.columns
    related_terms = []
    previous_score = nil
    record_batch.n_rows.times do |i|
      score = columns[1].get_value(i)
      break if score < 0.1
      previous_score ||= score
      break if (previous_score - score) > (score / 2.0)
      term = Groonga::Record.new(terms, columns[0].get_value(i)).key
      related_terms << [term, score]
    end
    next if related_terms.size < 2
    p related_terms
  end
ensure
  input_stream.close
end
