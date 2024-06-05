import json
from itertools import combinations
from openpyxl import Workbook

with open('entity_keywords_samaro.json', 'r') as json_file:
    entity_keywords = json.load(json_file)

filtered_entities = {entity: info for entity, info in entity_keywords.items() if info['count'] > 10}

sorted_entities = sorted(filtered_entities.items(), key=lambda x: x[1]['count'], reverse=True)

top_entity_names = [entity[0] for entity in sorted_entities]

entity_to_keywords = {entity: set(info['keywords']) for entity, info in sorted_entities}

entity_pairs = []
for entity1, entity2 in combinations(top_entity_names, 2):
    common_keywords = entity_to_keywords[entity1] & entity_to_keywords[entity2]
    count_common_keywords = len(common_keywords)
    entity_pairs.append((entity1, entity2, count_common_keywords))

workbook = Workbook()

sheet1 = workbook.active
sheet1.title = "Original Data"
sheet1.append(['Entity', 'Count', 'Keyword'])
for entity, info in entity_keywords.items():
    sheet1.append([entity, info['count'], info['keywords'][0]])
    for keyword in info['keywords'][1:]:
        sheet1.append(['', '', keyword])

sheet2 = workbook.create_sheet(title="Entity Pairs")
sheet2.append(['Entity1', 'Entity2', 'Common Keywords Count'])
for entity1, entity2, count in entity_pairs:
    sheet2.append([entity1, entity2, count])

excel_file_path = 'entity_keywords_with_pairs_samaro.xlsx'
workbook.save(excel_file_path)

print(f"Data has been written to {excel_file_path}")
