#!/usr/bin/env ruby

require "groonga"
require "arrow"

db_path = ARGV[0]
topics_path = ARGV[1]

Groonga::Database.open(db_path)
terms = Groonga["Words"]

Arrow::IO::MemoryMappedFile.open(topics_path, :read) do |input_stream|
  Arrow::IPC::StreamReader.open(input_stream) do |reader|
    reader.each do |record_batch|
      related_terms = []
      record_batch.each do |record|
        score = record["score"]
        next if score < 0.1
        term = Groonga::Record.new(terms, record["term_id"]).key
        related_terms << [term, score]
      end
      next if related_terms.size < 2
      p related_terms
    end
  end
end
