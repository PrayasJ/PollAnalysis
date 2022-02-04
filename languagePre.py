import pandas as pd
import os

class languagePre():
    def __init__(self):
        from whatthelang import WhatTheLang
        self.wtl = WhatTheLang()

    def parseInput(self, filename):
        language = []
        outData = pd.read_pickle(filename)

        text = outData['text'].to_list()
        language = self.wtl.predict_lang(text)

        outData['lang'] = language

        english = outData[outData['lang'] == 'en']
        hindi = outData[outData['lang'] == 'hi']

        cities = english.city.unique()
        locSplit_e = {elem: pd.DataFrame for elem in cities}

        for key in locSplit_e.keys():
            locSplit_e[key] = english[:][english.city == key]
        
            outpath = filename.split('.')[0]
            fname = outpath.split('/')[-1]
            outpath = 'data/geo/lang/'
            if os.path.exists(outpath + key + '_en.pkl'):
                tempdf = pd.read_pickle(outpath + key + '_en.pkl')
                locSplit_e[key] = pd.concat([tempdf, locSplit_e[key]], ignore_index=True)
            locSplit_e[key].to_pickle(outpath + key + '_en.pkl')

        cities = hindi.city.unique()
        locSplit_e = {elem: pd.DataFrame for elem in cities}

        for key in locSplit_e.keys():
            locSplit_e[key] = hindi[:][hindi.city == key]
        
            outpath = filename.split('.')[0]
            fname = outpath.split('/')[-1]
            outpath = 'data/geo/lang/'
            if os.path.exists(outpath + key + '_hi.pkl'):
                tempdf = pd.read_pickle(outpath + key + '_hi.pkl')
                locSplit_e[key] = pd.concat([tempdf, locSplit_e[key]], ignore_index=True)
            locSplit_e[key].to_pickle(outpath + key + '_hi.pkl')

        return outpath