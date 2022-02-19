import os
import csv
from collections import defaultdict

filenames = next(os.walk('data/geo/lang/freq/'), (None, None, []))[2]

freq = defaultdict(lambda: 0)

for filename in filenames:
    with open('data/geo/lang/freq/' + filename, 'r', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        
        for row in csvreader:
            if len(row) == 2:
                freq[row[0]] += int(row[1])
            else:
                freq[row[1]] += int(row[2])
            

freq = {k: v for k, v in sorted(freq.items(), key=lambda item: item[1], reverse=True)}

with open('data/geo/lang/freq/ALL.csv', 'w', encoding='utf-8') as outfile:
    for key in freq:
        outfile.write(key+','+str(freq[key])+'\n')