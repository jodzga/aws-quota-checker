import os
import pandas as pd
from fnmatch import fnmatch
    
root = "./"
filenames = []
pattern = "results-*.csv"
for path, subdirs, files in os.walk(root):
    for name in files:
        if fnmatch(name, pattern):
            filenames.append((os.path.join(path, name)))

combined_csv = pd.concat( [ pd.read_csv(f) for f in filenames ] )
combined_csv.to_csv( "combined-results.csv", index=False )
