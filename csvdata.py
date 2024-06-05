# import json
# import csv
# import pandas as pd

# with open('entity_keywords1.json', 'r') as json_file:
#     entity_keywords = json.load(json_file)

# # Define the CSV file path
# csv_file_path = 'entity_keywords.csv'

# # Limit to top 10,000 entities
# top_entities = dict(list(entity_keywords.items())[:10000])

# # Write the initial entity-keyword data to a CSV file
# with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
#     fieldnames = ['Entity', 'Count', 'Keyword']
#     writer = csv.writer(csv_file)

#     # Write the header
#     writer.writerow(fieldnames)

#     # Write the data
#     for entity, info in top_entities.items():
#         # Write the entity and its first keyword
#         writer.writerow([entity, info['count'], info['keywords'][0]])
#         # Write the remaining keywords without repeating the entity and count
#         for keyword in info['keywords'][1:]:
#             writer.writerow(['', '', keyword])

# # Create the entity matrix with counts of common keywords
# entities = list(top_entities.keys())
# matrix = pd.DataFrame(index=entities, columns=entities, data='0')

# for i, entity1 in enumerate(entities):
#     for j, entity2 in enumerate(entities):
#         if entity1 != entity2:
#             common_keywords_count = len(set(top_entities[entity1]['keywords']) & set(top_entities[entity2]['keywords']))
#             matrix.at[entity1, entity2] = common_keywords_count

# # Write the initial data and the matrix to an Excel file
# excel_file_path = 'entity_keywords_with_matrix.xlsx'

# with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
#     # Write the initial entity-keyword data to the first sheet
#     initial_data = pd.read_csv(csv_file_path)
#     initial_data.to_excel(writer, sheet_name='Entity_Keywords', index=False)
    
#     # Write the entity matrix to the second sheet
#     matrix.to_excel(writer, sheet_name='Entity_Matrix')

# print(f"Data has been written to {csv_file_path} and matrix to {excel_file_path}")

import json
from itertools import combinations
from openpyxl import Workbook

# Read the JSON data from the file
with open('entity_keywords_samaro.json', 'r') as json_file:
    entity_keywords = json.load(json_file)

# Sort entities by count and get the top 10,000 (for this example, we are using only 10)
sorted_entities = sorted(entity_keywords.items(), key=lambda x: x[1]['count'], reverse=True)
top_entities = sorted_entities[:10]

# Extract only the entity names
top_entity_names = [entity[0] for entity in top_entities]

# Create a dictionary for quick lookup of keywords for each entity
entity_to_keywords = {entity: set(info['keywords']) for entity, info in top_entities}

# Generate pairs and count common keywords
entity_pairs = []
for entity1, entity2 in combinations(top_entity_names, 2):
    common_keywords = entity_to_keywords[entity1] & entity_to_keywords[entity2]
    count_common_keywords = len(common_keywords)
    entity_pairs.append((entity1, entity2, count_common_keywords))

# Create a new Excel workbook and add sheets
workbook = Workbook()

# Add the original data to the first sheet
sheet1 = workbook.active
sheet1.title = "Original Data"
sheet1.append(['Entity', 'Count', 'Keyword'])
for entity, info in entity_keywords.items():
    sheet1.append([entity, info['count'], info['keywords'][0]])
    for keyword in info['keywords'][1:]:
        sheet1.append(['', '', keyword])

# Add the entity pairs with common keyword counts to the second sheet
sheet2 = workbook.create_sheet(title="Entity Pairs")
sheet2.append(['Entity1', 'Entity2', 'Common Keywords Count'])
for entity1, entity2, count in entity_pairs:
    sheet2.append([entity1, entity2, count])

# Save the workbook to a file
excel_file_path = 'entity_keywords_with_pairs_samaro.xlsx'
workbook.save(excel_file_path)

print(f"Data has been written to {excel_file_path}")