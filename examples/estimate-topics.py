#!/usr/bin/env python

import sys
sys.path.append("/home/kou/work/cpp/arrow/python")

import logging
import json

import scipy as sp
import pandas as pd
import pyarrow as A
from sklearn.decomposition import LatentDirichletAllocation

logging.basicConfig(level=logging.DEBUG)

LDA = LatentDirichletAllocation

metadata_path = sys.argv[1]
data_path = sys.argv[2]
topics_path = sys.argv[3]
if len(sys.argv) >= 5:
    n_documents = int(sys.argv[4])
else:
    n_documents = None

n_topics = 100

with open(metadata_path) as metadata_file:
    metadata = json.load(metadata_file)
if n_documents is None:
    n_documents = metadata["n_documents"]
else:
    n_documents = min(n_documents, metadata["n_documents"])
n_features = metadata["n_features"]

lda = LDA(n_topics=n_topics,
          learning_method="online",
          total_samples=n_documents,
          n_jobs=1)

with A.io.MemoryMappedFile(data_path, "rb") as source:
    reader = A.ipc.StreamReader(source)
    for i, batch in enumerate(reader):
        if i >= n_documents:
            break
        sys.stdout.write("\r%.3f%%" % ((i / n_documents) * 100))
        df = batch.to_pandas()
        corpus = sp.sparse.csr_matrix((df["score"].values,
                                       df["term_id"].values,
                                       [0, df["term_id"].size]),
                                      shape=(1, n_features))
        lda.partial_fit(corpus)
sys.stdout.write("\n")

def topic_to_df(topic):
    n_top_terms = 10
    return pd.DataFrame([[i, topic[i]]
                         for i in topic.argsort()[:-n_top_terms - 1:-1]],
                        columns=["term_id", "score"])

topic = lda.components_[0]
topic_df = topic_to_df(topic)
schema = A.RecordBatch.from_pandas(topic_df).schema
with open(topics_path, "wb") as sink:
    writer = A.ipc.StreamWriter(sink, schema)
    for topic in lda.components_:
        topic_df = topic_to_df(topic)
        topic_record_batch = A.RecordBatch.from_pandas(topic_df)
        writer.write_batch(topic_record_batch)
    writer.close()
