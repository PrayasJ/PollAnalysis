import tweepy
import time
import pandas as pd
from config import *


class TweetExtractor():

    def __init__(self):

        try:
            self.auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
            self.auth.set_access_token(access_token, access_token_secret)
            self.api = tweepy.API(self.auth, wait_on_rate_limit=True)

        except:
            print('Tweepy Authentication Failed.')

    def extract(self, queries = ['#UPElections2022'], filename: str = '') -> str:

        if filename == '':
            filename = time.strftime('%Y%m%d_%H%M%S')

        filename = 'data/extracted/' + filename + '.pkl'

        frames = []

        for query in queries:
            df = []
            tweetData = tweepy.Cursor(self.api.search_tweets, q=query, count=100)

            for tweet in tweetData.items():
                try:
                    df.append([tweet.created_at, tweet.user.screen_name,
                                    tweet.user.location, tweet.text])

                except:
                    print('Error in Extraction.')
            df = pd.DataFrame(df, columns=['created_at', 'screen_name', 'location', 'text'])
            frames.append(df)
        
        df = pd.concat(frames)
        df.drop_duplicates(inplace=True, ignore_index=False)
        #df.to_csv(filename, encoding='utf-8', index=False)
        df.to_pickle(filename)

        print('Extraction Complete!')
        return filename
