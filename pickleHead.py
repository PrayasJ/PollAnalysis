import pandas as pd
import sys

fname = sys.argv[1]

df = pd.read_pickle(fname)

print(df.head(5))