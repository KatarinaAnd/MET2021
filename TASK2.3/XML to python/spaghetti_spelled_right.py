import numpy as np
import pandas as pd
'''
f = open('spagetthi.txt', 'r')
lines = f.readlines()
f.close()

string = lines[0].split(',')
new_string = [string[0].replace('</entry_id>','').replace('[<entry_id>','').strip(' ')]
for i in range(1, len(string)-1):
    new_thing = string[i].replace('</entry_id>','').replace('<entry_id>','').strip(' ')
    new_string.append(new_thing)

#print(string[2].replace('</entry_id>','').replace('<entry_id>','').strip(' '))

df = pd.DataFrame(new_string)
print(df)
'''
standard_names = pd.read_pickle('pizza.pkl')
standard_names.columns = ['standard_names']
frost = pd.read_pickle('topping.pkl')
frost.columns = ['frost']
new = standard_names
new['frost'] = frost
new['truefalse'] = new.frost.isin(new.standard_names).astype(int)
#print(new)
truestuff = new[new.truefalse != 0]
truestuff = truestuff['frost']
truestuff = truestuff.reset_index().drop(columns=['index'])
falsestuff = new[new.truefalse == 0]
falsestuff = falsestuff['frost']
falsestuff = falsestuff.reset_index().drop(columns=['index'])

print(falsestuff)