from datetime import time, datetime
from dask import dataframe as df
from dask.dataframe.utils import make_meta
import json
import pandas as pd

class Devolframe:
    propsmap = {
        '@timestamp': datetime.today(),
        '@version': 0,
        'event': {'dataset': ''},
        'host': {'name': ''},
        'name': '',
        'dataset': '',
        'cpu': {'cores': 0, 'idle': {'pct': 0.0}, 'user': {'pct': 0.0}, 'system': {'pct': 0.0}},
        'cores': 0,
        'idle': {'pct': 0.0},
        'user': {'pct': 0.0},
        'system': {'pct': 0.0},
        'pct': 0.0,
        'network': {'out': {'bytes': 0}, 'in': {'bytes': 0}},
        'out': {'bytes': 0},
        'in': {'bytes': 0},
        'bytes': 0,
    }
    
    
    def __init__(self, csv='', block=25_000_000):
        converters = {
            '@timestamp':Devolframe.timeParser,
            'event':Devolframe.jsonParserEve,
            'host':Devolframe.jsonParserHos,
            'system':Devolframe.jsonParserSys,
        }
        self.dv = df.read_csv(csv, blocksize=block, converters=converters, header=0)
    
    def cleanUp(self):
        pass
    
    def getProp(self, prop, n=None, parts=1):
        return self.dv.head(n if n != None else 10, npartitions=parts)[prop]
    
    def findMissing(self, prop, callback, n=None, targetProp=None, parts=1):
        targetProp = prop if targetProp == None else targetProp
        try: return self.dv[callback(self.dv, prop)].head(n if n != None else 0, npartitions=parts)[targetProp]
        except: return self.getProp(targetProp, n)
        
    def findWhere(self, targetProp, value, callback, n=None, parts=1):
        try: return self.dv[callback(self.dv[targetProp], value)].head(n if n != None else 0, npartitions=parts)[targetProp]
        except: return self.getProp(targetProp, n)
            
    def genCols(self):
        # cleanjson = lambda d, prop: d.get(prop) if isinstance(d, dict) else d if isinstance(d, (int, float, datetime)) else json.loads(d.replace('\'', '"')).get(prop) if isinstance(d, str) else d
        
        def cleanjson(d, prop):
            if d == None:
                return Devolframe.propsmap[prop]
            if isinstance(d, (float, int, datetime)):
                return d
            if isinstance(d, dict):
                return d.get(prop)
            if isinstance(d, str):
                try:
                    return json.loads(d.replace('\'', '"')).get(prop)
                except:
                    return Devolframe.propsmap[prop]
        
        self.dv['rehost'] = self.dv['host'].apply(lambda d: cleanjson(d, 'name'), meta=('name', 'str'))
        self.dv['reevent'] = self.dv['event'].apply(lambda d: cleanjson(d, 'dataset'), meta=('dataset', 'str'))
        self.dv['cores'] = self.dv['system'].apply(lambda d: cleanjson(cleanjson(d, 'cpu'), 'cores'), meta=('cores', 'int'))
        self.dv['idle'] = self.dv['system'].apply(lambda d: cleanjson(cleanjson(cleanjson(d, 'cpu'), 'idle'), 'pct'), meta=('pct', 'float'))
        self.dv['user'] = self.dv['system'].apply(lambda d: cleanjson(cleanjson(cleanjson(d, 'cpu'), 'user'), 'pct'), meta=('pct', 'float'))
        self.dv['sys'] = self.dv['system'].apply(lambda d: cleanjson(cleanjson(cleanjson(d, 'cpu'), 'system'), 'pct'), meta=('pct', 'float'))
        self.dv['out'] = self.dv['system'].apply(lambda d: cleanjson(cleanjson(cleanjson(d, 'network'), 'out'), 'bytes'), meta=('bytes', 'int'))
        self.dv['in'] = self.dv['system'].apply(lambda d: cleanjson(cleanjson(cleanjson(d, 'network'), 'in'), 'bytes'), meta=('bytes', 'int'))
    
    @staticmethod
    def jsonParser(data, fail):
        if isinstance(data, dict): return data
        try: return json.loads(f'"{data}"')
        except: return fail
        
    @staticmethod
    def jsonParserSys(data):
        return Devolframe.jsonParser(data, Devolframe.propsmap['cpu'])
        
    @staticmethod
    def jsonParserEve(data):
        return Devolframe.jsonParser(data, Devolframe.propsmap['event'])
    
    @staticmethod
    def jsonParserHos(data):
        return Devolframe.jsonParser(data, Devolframe.propsmap['host'])

    @staticmethod
    def timeParser(data):
        if isinstance(data, datetime): return data
        try: return datetime.fromisoformat(data[:-1])
        except: return Devolframe.propsmap['@timestamp']