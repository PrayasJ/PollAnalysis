import sys

from tweetExtractor import TweetExtractor
from geoParser import GeoParser
from languagePre import languagePre
from tokenizePoll import tokenizePoll

class whysomean():

    commands = {
        'all' : ['--all', '-a'],
        'extract': ['--extract', '-e'],
        'geo': ['--geo', '-g'],
        'freq': ['--freq', '-f']
    }

    def __init__(self):

        argv = sys.argv[1:]

        if argv[0].lower() in self.commands['all']:
            value = self.argParser(argv)
            extractor = TweetExtractor()
            filename = ''
            if value == None:
                filename = extractor.extract()
            else:
                value = value.split(',')
                filename = extractor.extract(value)
            geoParser = GeoParser()
            geoParser.parse(filename)

            lp = languagePre()
            filename = lp.parseInput(filename['known'])

            tp = tokenizePoll()
            tp.tokenize(filename['english'])

        else:
            if argv[0].lower() in self.commands['extract']:
                extractor = TweetExtractor()
                value = self.argParser(argv)
                if value == None:
                    extractor.extract()
                else:
                    value = value.split(',')
                    extractor.extract(value)
            elif argv[0].lower() in self.commands['geo']:
                value = self.argParser(argv)
                if value == None:
                    print('Pass some filename please uwu.')
                else:
                    geoParser = GeoParser()
                    geoParser.parse(value)
            elif argv[0].lower() in self.commands['freq']:
                value = self.argParser(argv)
                if value == None:
                    print('Pass some filename please uwu.')
                else:
                    lp = languagePre()
                    value = lp.parseInput(value)

                    tp = tokenizePoll()
                    tp.tokenize(value['english'])

    def argParser(self, argv):
        if len(argv) == 2:
            if argv[1][0] == '-':
                return None
            else:
                return argv[1]
        else:
            return None

if __name__ == '__main__':
    whysomean()