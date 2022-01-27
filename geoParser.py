import pandas as pd
from geopy.geocoders import Nominatim
import dask.dataframe as dd
from tqdm import tqdm
from dask.diagnostics import ProgressBar

ProgressBar().register()

data = {}

locationDict = {}
count = 30

class GeoParser():
    def __init__(self):
        tqdm.pandas()
        self.geolocator = Nominatim(user_agent="http")
        pd.options.mode.chained_assignment = None

    def getCity(self, address):
        if 'city' in address:
            return address['city']
        elif 'county' in address:
            return address['county']
        else:
            return 'outside'
    
    def geoLocate(self, location):
        if location not in locationDict:
            locationDict[location] = self.geolocator.geocode(location, addressdetails=True, timeout = None)
        return locationDict[location]

    def parse(self, filename:str, place:str = 'Uttar Pradesh'):
        df = pd.read_pickle(filename)
        #df = df.head(count)    
        
        ddf = dd.from_pandas(df, npartitions=30)
        df['location'] = ddf.map_partitions(lambda loc: loc['location'].progress_apply(lambda l: self.geoLocate(l))).compute()
        known = df.dropna()
        unknown = df[~df.index.isin(known.index)]

        known['state'] = known['location'].apply(lambda x: x.raw['address']['state'] if 'state' in x.raw['address'] else 'outside')
        known['city'] = known['location'].apply(lambda x: self.getCity(address=x.raw['address']))
        known.loc[known['state'] != place, 'city'] = 'outside'
        
        rows = known.loc[known['state'] != place, :]
        unknown = unknown.append(rows, ignore_index=True)
        
        known = known.drop(rows.index)
        known = known.drop('location', axis=1)

        outfile = filename.split('/')[-1]
        outfile = outfile.split('.')[0] 
        
        known.to_pickle('data/geo/' + outfile + '_known.pkl')
        unknown.to_pickle('data/geo/' + outfile + '_unknown.pkl') 
        
        return {'known' : 'data/geo/' + outfile + '_known.pkl',
                'unknown' : 'data/geo/' + outfile + '_unknown.pkl'}