import csv
from pydantic import BaseModel
from typing import List
from llama_index.program.openai import OpenAIPydanticProgram


class KeywordEntities(BaseModel):
    keyword: str
    entities: List[str]

prompt_template_str = """\
Role: SEO Specialist. \
Task: List important granular entities in each keyword. \
Keyword: {keyword}
"""

csv_file_path = "IIRA keywords database - All Keywords.csv"
keywords = []
with open(csv_file_path, mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)
    next(reader)
    count = 0
    for row in reader:
        keywords.append(row[0])
        count += 1
        if count >= 50:
            break

program = OpenAIPydanticProgram.from_defaults(output_cls=KeywordEntities, prompt_template_str=prompt_template_str, verbose=False)

outputs = []

csv_file_path = "keyword_entities_01.csv"
with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(["Keyword", "Entity 1", "Entity 2", "Entity 3", "Entity 4", "Entity 5"])
    for keyword in keywords:
        output = program(keyword=keyword,description="entities for keyword")
        entities = output.entities[:5]
        writer.writerow([output.keyword] + entities)

print(f"CSV file saved successfully: {csv_file_path}")
