from datetime import time, datetime
from dask import dataframe as df
import json
import pandas as pd

class Devolframe:
    def __init__(self, csv, block):
        converters = {
            '@timestamp':Devolframe.timeParser,
            'event':Devolframe.jsonParser,
            'host':Devolframe.jsonParser,
            'system':Devolframe.jsonParser,
        }
        self.dv = df.read_csv(csv, blocksize=block, converters=converters, header=0)
    
    def cleanUp(self):
        pass
    
    def getProp(self, prop, n = None, parts=1):
        return self.dv.head(n if n != None else 10, npartitions=parts)[prop]
    
    def findMissing(self, prop, callback, n = None, targetProp=None, parts=1):
        targetProp = prop if targetProp == None else targetProp
        try:
            return self.dv[callback(self.dv, prop)].head(n if n != None else 0, npartitions=parts)[targetProp]
        except:
            return self.getProp(targetProp, n)
        
    def findWhere(self, targetProp, value, callback, n = None, parts=1):
        try:
            return self.dv[callback(self.dv[targetProp], value)].head(n if n != None else 0, npartitions=parts)[targetProp]
        except:
            return self.getProp(targetProp, n)
    
    @staticmethod
    def jsonParser(data):
        if isinstance(data, object.__name__()):
            return data
        try: return json.loads(f'"{data}"')
        except: return None

    @staticmethod
    def timeParser(data):
        if isinstance(data, datetime.__name__()):
            return data
        try: return datetime.fromisoformat(data[:-1])
        except: return None