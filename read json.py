import  json

with open('data.txt') as f:
    data = json.load(f)
print(json.dumps(data,sort_keys=True,indent=4))