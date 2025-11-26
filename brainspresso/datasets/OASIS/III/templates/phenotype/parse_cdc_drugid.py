import requests
import json
from pathlib import Path

try:
    from lxml import etree
except ImportError:
    raise ImportError('lxml required to parse the CDC drug id list. '
                      '`pip install lxml`')

url = "https://wwwn.cdc.gov/Nchs/Nhanes/1999-2000/RXQ_DRUG.htm"
html = requests.Session().get(url).content

drugmap = {}

root = etree.HTML(html)
for elem in root.findall('body/div/div/div'):
    if elem.attrib.get('id', '') != 'Appendix':
        print(elem.attrib.get('id', ''), '!=', 'Appendix')
        continue
    title = elem.find('h2')
    if title is None:
        continue
    if not title.attrib.get('id', '').startswith('Appendix_2'):
        print(title.attrib.get('id', ''), '!=', 'Appendix_2')
        continue
    table = elem.find('table/tbody')

    rows = iter(table)
    headers = [col.text for col in next(rows)]
    for row in rows:
        values = [col.text for col in row]
        drugmap[values[0]] = values[1]

print(json.dumps(drugmap, indent=4))

with open(Path(__file__).parent / 'cdc_drugid.json', 'wt') as f:
    json.dump(drugmap, f, indent=2)
