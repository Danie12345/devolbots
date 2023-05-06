import json


limit = 100
with open('metricbeabt-so-28042023.csv') as file:
    for line in file.readlines():
        # x = line.split("{'cpu':")
        a,b,c,d,*e = line.split(',')
        e = ''.join(e)[1:-2].replace('} \'', '}, \'')
        e = json.loads(f"\"{e}\"")
        try:
            print (a)
            print (e)
        except: pass
        
        limit -= 1
        if limit < 0:
            break



# x = json.loads(mivarcontextodejson)

