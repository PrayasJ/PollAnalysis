import nltk
from nltk.tokenize import word_tokenize
import multiprocessing as mp
import tqdm
import pandas as pd
import numpy as np
from collections import defaultdict

from nltk.stem import WordNetLemmatizer

class tokenizePoll():
    
    def tokenize_func(self, tweet):
        try:
            words = word_tokenize(tweet)
            words = [self.lemmatizer.lemmatize(word.lower()) for word in words]
            return nltk.pos_tag(words)
        except:
            return ('null', 'null')

    def flatten(self, t):
        return [item for sublist in t for item in sublist]

    def __init__(self):
        self.lemmatizer = WordNetLemmatizer()
        self.tags = ['FW', 'JJR', 'JJS', 'NN', 'NNS', 'NNP', 'NNPS', 'PDT', 'RB', 'RBR', 'RBS', 'RP', 'VB', 'VBG', 'VBD', 'VBN', 'VBP', 'VBZ']

    def tokenize(self, filename):
        wordSet = []

        df = pd.read_pickle(filename)

        text = df['text'].to_list()
        wordBox = []

        with mp.Pool() as pool:
            #wordBox = pool.map(self.tokenize_func, text)
            for _ in tqdm.tqdm(pool.map(self.tokenize_func, text), total=len(text)):
                wordBox.append(_)
        
        wordSet = self.flatten(wordBox)
        filtereddWordSet = [word[0] for word in wordSet if word[1] in self.tags]
        
        wordCount = defaultdict(int)

        for word in filtereddWordSet: wordCount[word] += 1
        wordSortedIndex = sorted(wordCount, key= wordCount.get, reverse= True)

        outf = filename.split('.')[0]

        with open(outf + '_frequency.txt', 'w', encoding='utf-8') as outfile:
            for word in wordSortedIndex:
                outfile.write(word + ',' + str(wordCount[word]) + '\n')

        return outf + '_frequency.txt'