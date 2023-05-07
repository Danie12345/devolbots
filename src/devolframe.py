from datetime import time, datetime
from dask import dataframe as df
from dask.dataframe.utils import make_meta
import json
import pandas as pd

class Devolframe:
    def __init__(self, csv='', block=25_000_000):
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
        
    def genCols(self):
        cleanjson = lambda d, prop: d.get(prop) if isinstance(d, dict) else d if isinstance(d, (int, float)) else None if d == None else json.loads(d.replace('\'', '"')).get(prop)
        self.dv['timestamp'] = self.dv['@timestamp']
        self.dv['version'] = self.dv['@version']
        self.dv['rehost'] = self.dv['host'].apply(lambda d: cleanjson(d, 'name'), meta=('name', 'str'))
        self.dv['reevent'] = self.dv['event'].apply(lambda d: cleanjson(d, 'dataset'), meta=('dataset', 'str'))
        self.dv['cores'] = self.dv['system'].apply(lambda d: cleanjson(cleanjson(d, 'cpu'), 'cores'), meta=('cores', 'int'))
        self.dv['idle'] = self.dv['system'].apply(lambda d: cleanjson(cleanjson(cleanjson(d, 'cpu'), 'idle'), 'pct'), meta=('pct', 'float'))
        self.dv['user'] = self.dv['system'].apply(lambda d: cleanjson(cleanjson(cleanjson(d, 'cpu'), 'user'), 'pct'), meta=('pct', 'float'))
        self.dv['sys'] = self.dv['system'].apply(lambda d: cleanjson(cleanjson(cleanjson(d, 'cpu'), 'system'), 'pct'), meta=('pct', 'float'))
        self.dv['out'] = self.dv['system'].apply(lambda d: cleanjson(cleanjson(cleanjson(d, 'network'), 'out'), 'bytes'), meta=('bytes', 'int'))
        self.dv['in'] = self.dv['system'].apply(lambda d: cleanjson(cleanjson(cleanjson(d, 'network'), 'in'), 'bytes'), meta=('bytes', 'int'))
        # self.dv.drop(columns=['@timestamp', '@version', 'host', 'event', 'system']).compute()
        # mask = self.dv['out'].notnull()
        return self.dv.head(10)#.loc[mask].compute()
        
    
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