from whatthelang import WhatTheLang
import pandas as pd

class languagePre():
    def __init__(self):
        self.wtl = WhatTheLang()

    def parseInput(self, filename):
        language = []
        outData = pd.read_pickle(filename)

        text = outData['text'].to_list()
        language = self.wtl.predict_lang(text)

        outData['lang'] = language

        english = outData[outData['lang'] == 'en']
        hindi = outData[outData['lang'] == 'hi']

        outpath = filename.split('.')[0]
        outpath = 'data/geo/lang/' + outpath.split('/')[-1]

        english.to_pickle(outpath + '_en.pkl')
        hindi.to_pickle(outpath + '_hi.pkl')

        return {
            'hindi' : outpath + '_hi.pkl',
            'english' : outpath + '_en.pkl'
        }