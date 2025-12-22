import pandas as pd
import json
import datetime

df = pd.read_excel('data.xlsx')
df['id'] = df.index + 1
df['created_at'] = str(datetime.datetime.now())
df['tags'] = [[] for _ in range(len(df))]
df['status_id'] = 3
new_probes = df.to_dict('records')

with open('data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
    
data['probes'] = new_probes

with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2, ensure_ascii=False)