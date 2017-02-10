#!/usr/bin/env ruby

require "groonga"
require "arrow"

db_path = ARGV[0]
metadata_output_path = ARGV[1]
data_output_path = ARGV[2]
use_tfidf = (ARGV[3] != "tf")
use_filter = (ARGV[4] != "raw")

Groonga::Database.open(db_path)

Groonga::Schema.define do |schema|
  schema.create_table("Words",
                      :type => :patricia_trie,
                      :key_type => "ShortText",
                      :default_tokenizer => "TokenMecab",
                      :normalizer => "NormalizerAuto") do |table|
    table.index("Entries.document")
  end
end

n_entries = Groonga["Entries"].size
too_many_much_threshold = n_entries * 0.25
too_less_much_threshold = n_entries * 0.001

bow = {}
index = Groonga["Words.Entries_document"]
max_term_id = 0
index.table.open_cursor(:order_by => :id) do |table_cursor|
  table_cursor.each do |term|
    n_match_documents = index.estimate_size(term)
    # p [term.key, n_match_documents, (n_match_documents / n_entries.to_f)]
    if use_filter
      if n_match_documents <= too_less_much_threshold
        p [:skip, :too_less, term.key, n_match_documents]
        next
      end
      if n_match_documents >= too_many_much_threshold
        p [:skip, :too_many, term.key, n_match_documents]
        next
      end
    end
    max_term_id = [max_term_id, term.id].max
    df = Math.log(n_entries.to_f / n_match_documents)
    index.open_cursor(term.id,
                      :with_position => false) do |index_cursor|
      index_cursor.each(:reuse_posting_object => true) do |posting|
        next unless posting.record.version.key == "2.4.0"
        bow[posting.record_id] ||= []
        if use_tfidf
          score = posting.term_frequency / df
        else
          score = posting.term_frequency
        end
        bow[posting.record_id] << [posting.term_id, score]
      end
    end
    break
  end
end

File.open(metadata_output_path, "w") do |metadata_file|
  metadata_file.puts({
                       "n_documents" => bow.size,
                       "n_features" => max_term_id,
                     }.to_json)
end

Arrow::IO::FileOutputStream.open(data_output_path, false) do |output_stream|
  term_id_field = Arrow::Field.new("term_id", :uint32)
  score_field = Arrow::Field.new("score", :double)
  schema = Arrow::Schema.new([term_id_field, score_field])
  Arrow::IPC::StreamWriter.open(output_stream, schema) do |writer|
    bow.each do |record_id, words|
      term_ids = Arrow::UInt32Array.new(words.collect(&:first))
      scores = Arrow::DoubleArray.new(words.collect(&:last))
      record_batch = Arrow::RecordBatch.new(schema,
                                            words.size,
                                            [term_ids, scores])
      writer.write_record_batch(record_batch)
    end
  end
end
