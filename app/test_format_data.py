import json
import numpy as np

param_list = []

with open('data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
    
probes = data['probes']
for probe in probes:

    for key, value in probe.items():
        
        param_list.append(key)
        
print(np.unique(param_list))

