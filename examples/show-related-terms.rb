#!/usr/bin/env ruby

require "groonga"
require "arrow"

db_path = ARGV[0]
topics_path = ARGV[1]

Groonga::Database.open(db_path)
terms = Groonga["Words"]
index = Groonga["Words.Entries_document"]

Arrow::IO::MemoryMappedFile.open(topics_path, :read) do |input_stream|
  Arrow::IPC::StreamReader.open(input_stream) do |reader|
    reader.each do |record_batch|
      related_terms = []
      previous_score = nil
      # p :topic_raw
      # record_batch.each do |record|
      #   term = Groonga::Record.new(terms, record["term_id"]).key
      #   p [record["term_id"], record["score"], term, index.estimate_size(term)]
      # end
      record_batch.each do |record|
        score = record["score"]
        break if score < 0.1
        previous_score ||= score
        break if (previous_score - score) > (previous_score / 2.0)
        previous_score = score
        term = Groonga::Record.new(terms, record["term_id"]).key
        related_terms << [term, score]
      end
      next if related_terms.size < 2
      p :topic
      related_terms.each do |term|
        p term
      end
    end
  end
end
