import pandas as pd
import ast

filename = "data/extracted/20220122_190725.csv"

def _parse_bytes(field):
    """ Convert string represented in Python byte-string literal b'' syntax into
        a decoded character string - otherwise return it unchanged.
    """
    result = field
    try:
        result = ast.literal_eval(field)
    finally:
        return result.decode() if isinstance(result, bytes) else field

df = pd.read_csv(filename, encoding='iso-8859-1', names=['created_at', 'screen_name', 'location', 'text'])

df['text'] = df['text'].apply(lambda txt: _parse_bytes(txt))

outpath = filename.split('.')[0] + '.pkl'

df.to_pickle(outpath)