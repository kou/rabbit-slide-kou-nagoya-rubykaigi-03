#!/usr/bin/env python

import sys
sys.path.append("/home/kou/work/cpp/arrow/python")

import pandas as pd
import numpy as np
import pyarrow as A

df = pd.DataFrame({'foo': [1.5, 3.0]})
batch = A.RecordBatch.from_pandas(df)
sink = A.io.OSFile("/tmp/xxx", "w")
writer = A.ipc.FileWriter(sink, batch.schema)
writer.write_batch(batch)
writer.close()
