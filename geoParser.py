import pandas as pd
from geopy.geocoders import Nominatim
import dask.dataframe as dd
from tqdm import tqdm
from dask.diagnostics import ProgressBar
import numpy as np
from os.path import exists
import os

class GeoParser():
    def __init__(self):
        ProgressBar().register()
        tqdm.pandas()
        self.locationDict = {}
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
        if location not in self.locationDict:
            self.locationDict[location] = self.geolocator.geocode(location, addressdetails=True, timeout = None)
        return self.locationDict[location]

    def parse(self, filename:str, place:str = 'Uttar Pradesh'):
        df = pd.read_pickle(filename)

        outfile = filename.split('/')[-1]
        outfile = outfile.split('.')[0]

        for i, item in enumerate(np.array_split(df, 10)):
            item = item.reset_index(drop=True)
            if exists("data/geo/"+outfile+"_"+str(i)+"_known.pkl"):
                pass
            else:
                ddf = dd.from_pandas(item, npartitions=30)
                item['location'] = ddf.map_partitions(lambda loc: loc['location'].apply(lambda l: self.geoLocate(l))).compute()
                known = item.dropna()
                unknown = item[~item.index.isin(known.index)]

                known['state'] = known['location'].apply(lambda x: x.raw['address']['state'] if 'state' in x.raw['address'] else 'outside')
                known['city'] = known['location'].apply(lambda x: self.getCity(address=x.raw['address']))
                known.loc[known['state'] != place, 'city'] = 'outside'
                
                rows = known.loc[known['state'] != place, :]
                unknown = unknown.append(rows, ignore_index=True)
                
                known = known.drop(rows.index)
                known = known.drop('location', axis=1)

                known.to_pickle('data/geo/' + outfile +'_'+str(i)+ '_known.pkl')
                unknown.to_pickle('data/geo/' + outfile +'_'+str(i)+ '_unknown.pkl') 

        allDf_kn = []
        for i in range(10):
            temp = pd.read_pickle('data/geo/' + outfile +'_'+str(i)+ '_known.pkl')
            allDf_kn.append(temp)
            os.remove('data/geo/' + outfile +'_'+str(i)+ '_known.pkl')
        known = pd.concat(allDf_kn)
        known.to_pickle('data/geo/' + outfile + '_known.pkl')

        allDf_un = []
        for i in range(10):
            temp = pd.read_pickle('data/geo/' + outfile +'_'+str(i)+ '_unknown.pkl')
            allDf_un.append(temp)
            os.remove('data/geo/' + outfile +'_'+str(i)+ '_unknown.pkl')
        unknown = pd.concat(allDf_un)
        unknown.to_pickle('data/geo/' + outfile + '_unknown.pkl')

        
        return {'known' : 'data/geo/' + outfile + '_known.pkl', 
                'unknown' : 'data/geo/' + outfile + '_unknown.pkl'}