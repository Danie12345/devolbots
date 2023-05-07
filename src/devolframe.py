from datetime import time, datetime
from dask import dataframe as df
from dask.dataframe.utils import make_meta
import json
import pandas as pd

class Devolframe:
    def __init__(self, csv='', block=25_000_000, new=False):
        if new:
            # cols = ['timestamp', 'host']
            
            frame = pd.DataFrame({'timestamp': [datetime.fromisoformat('2023-03-13T07:04:31.079')]})
            d = df.from_pandas(frame, npartitions=1)
            
            self.dv = d
            return
        converters = {
            '@timestamp':Devolframe.timeParser,
            'event':Devolframe.jsonParser,
            'host':Devolframe.jsonParser,
            'system':Devolframe.jsonParser,
        }
        self.dv = df.read_csv(csv, blocksize=block, converters=converters, header=0)
    
    def cleanUp(self):
        pass
    
    def getProp(self, prop, n=None, parts=1):
        return self.dv.head(n if n != None else 10, npartitions=parts)[prop]
    
    def findMissing(self, prop, callback, n=None, targetProp=None, parts=1):
        targetProp = prop if targetProp == None else targetProp
        try:
            return self.dv[callback(self.dv, prop)].head(n if n != None else 0, npartitions=parts)[targetProp]
        except:
            return self.getProp(targetProp, n)
        
    def findWhere(self, targetProp, value, callback, n=None, parts=1):
        try:
            return self.dv[callback(self.dv[targetProp], value)].head(n if n != None else 0, npartitions=parts)[targetProp]
        except:
            return self.getProp(targetProp, n)
        
    def genNew(self):
        # newDv = Devolframe(new=True)
        # newDv.dv['timestamp'] = self.dv['@timestamp']
        # newDv.dv['host'] = self.dv['host']['name']
        # return newDv.dv.head(10)
        self.dv['timestamp'] = self.dv['@timestamp']
        self.dv['host'] = self.dv['host']['name']
        
        return self.dv.head(10)['host']
        
    
    @staticmethod
    def jsonParser(data):
        if isinstance(data, object):
            return data
        try: return json.loads(f'"{data}"')
        except: return None

    @staticmethod
    def timeParser(data):
        if isinstance(data, datetime):
            return data
        try: return datetime.fromisoformat(data[:-1])
        except: return None