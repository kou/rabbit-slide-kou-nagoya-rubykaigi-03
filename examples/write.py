#!/usr/bin/env python

import pandas as pd
import numpy as np
import pyarrow as A
import pyarrow.io as aio
import pyarrow.ipc as ipc

df = pd.DataFrame({'foo': [1.5]})
batch = A.RecordBatch.from_pandas(df)
sink = aio.OSFile("/tmp/xxx", "w")
writer = ipc.ArrowFileWriter(sink, batch.schema)
writer.write_record_batch(batch)
writer.close()
