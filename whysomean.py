import sys

class whysomean():

    commands = {
        'all' : ['--all', '-a'],
        'extract': ['--extract', '-e'],
        'geo': ['--geo', '-g'],
        'freq': ['--freq', '-f'],
        'freq1': ['--freq1', '-f1']
    }

    def __init__(self):

        argv = sys.argv[1:]

        if argv[0].lower() in self.commands['all']:
            from tweetExtractor import TweetExtractor
            from geoParser import GeoParser
            from languagePre import languagePre
            from tokenizePoll import tokenizePoll

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
            tp.tokenize(filename)

        else:

            if argv[0].lower() in self.commands['extract']:
                from tweetExtractor import TweetExtractor

                extractor = TweetExtractor()
                value = self.argParser(argv)
                if value == None:
                    extractor.extract()
                else:
                    value = value.split(',')
                    extractor.extract(value)
            elif argv[0].lower() in self.commands['geo']:
                from geoParser import GeoParser

                value = self.argParser(argv)
                if value == None:
                    print('Pass some filename please uwu.')
                else:
                    geoParser = GeoParser()
                    geoParser.parse(value)
            elif argv[0].lower() in self.commands['freq']:
                from languagePre import languagePre

                value = self.argParser(argv)
                if value == None:
                    print('Pass some filename please uwu.')
                else:
                    lp = languagePre()
                    value = lp.parseInput(value)
                    
                    # tp = tokenizePoll()
                    # tp.tokenize(value)
            elif argv[0].lower() in self.commands['freq1']:
                from tokenizePoll import tokenizePoll
                
                tp = tokenizePoll()
                tp.tokenize()

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