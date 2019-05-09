import json

vara = []
with open("notifiable-infectious-diseases-report-2016-week-17_package.json", "r") as f:
    vara.append(json.load(f))
with open("scheduled-historic-monument-areas_package.json", "r") as f:
    vara.append(json.load(f))

keys = {'TOP': [var['result'] for var in vara]}

for n, var in enumerate(vara):
    for key, item in var['result'].items():
        if type(item) is dict:
            if key not in keys:
                keys[key] = [{} for _ in vara]
            var['result'][key] = "[BELOW]"
            keys[key][n] = item
        elif type(item) is list and item:
            if key not in keys:
                keys[key] = [{} for _ in vara]
            var['result'][key] = "[LIST LIKE BELOW]"
            if type(item[0]) is dict:
                keys[key][n] = item[0]
            elif type(item[0]) is list:
                keys[key][n] = {i: v for i, v in enumerate(item[0])}
            else:
                keys[key][n] = {'': item[0]}

print("Key" + ",Example Value" * len(vara) + "\n")
for key, dct in keys.items():
    print("{},".format(key.upper()))
    ky = set()
    for d in dct:
        ky |= set(d.keys())
    ky = sorted(list(ky))
    for k in ky:
        row = "%s," % k
        for d in dct:
            if k in d:
                v = d[k]
                if not v:
                    v = "[EMPTY]"
                else:
                    v = str(v).replace(',', '\\,').replace('\r\n', '// ')
            else:
                v = ""
            row += v + ","
        print(row)
    print(",")
