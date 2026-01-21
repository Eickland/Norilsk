import json

with open('data/data.json', 'r', encoding='utf-8') as file:
    data = json.load(file)
    
probes = data.get('probes')

for probe in probes:
    if (probe['name'][-2] == "B") and (len(probe['name']) == 6):
        probe['sample_mass'] = probe['Масса твердого (g)']
        print(1)

with open('data/data.json', 'w', encoding='utf-8') as file:
    json.dump(data, file, ensure_ascii=False, indent=2)