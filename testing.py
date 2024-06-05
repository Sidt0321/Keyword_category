# import csv
# from pydantic import BaseModel
# from typing import List
# from llama_index.program.openai import OpenAIPydanticProgram
# import asyncio
# from concurrent.futures import ThreadPoolExecutor
# from functools import partial
# import json

# class EntitiesInText(BaseModel):
#     primary_entities: List[str]

# prompt_template_str = """
# Role: SEO Specialist. 
# Task: List all the primary entities from given text below similar to examples below.
# Instructions: 
# 1. First read the whole text and understand what entities it contains and then only give response. 
# 2. Give only granular primary entities.
# 3. Exclude all adjectives in the entities.
# 4. Dont use actions in tHe entities.

# ---------------------------------
# Given text below: {keyword}
# primary_entities: 
# """

# # Split the list into chunks
# def chunks(lst, n):
#     """Yield successive n-sized chunks from lst."""
#     for i in range(0, len(lst), n):
#         yield lst[i:i + n]

# # Async function to call the program
# async def async_openai_call(program, keyword):
#     loop = asyncio.get_running_loop()
#     with ThreadPoolExecutor() as pool:
#         func = partial(program, keyword=keyword, description="First read the whole text and list all primary entities in the text")
#         return await loop.run_in_executor(pool, func)



# async def get_keywords_entities(program, keyword):
#     output = await async_openai_call(program, keyword)

#     if output:
#         # print (output.dict())
#         return {'keyword': keyword, 'entities': output.primary_entities}


# async def process_batch(program, batch):
#     tasks = [get_keywords_entities(program, keyword) for keyword in batch]
#     entities_data = await asyncio.gather(*tasks)

#     return entities_data

# async def main():
#     csv_file_path = "all_keywords.txt"
#     keywords = []

#     with open(csv_file_path, mode='r') as file:
#         keywords = file.readlines()
#         keywords = [keyword.strip() for keyword in keywords]

#     keywords = keywords[:1000]
    
#     program = OpenAIPydanticProgram.from_defaults(output_cls=EntitiesInText, prompt_template_str=prompt_template_str, verbose=False)
    

#     for batch in chunks(keywords, 25):
#         data = await process_batch(program, batch)



#     # map entities to keywords
#     entity_keywords = {}
#     for row in data:
#         for entity in row['entities']:
#             if entity in entity_keywords.keys():
#                 entity_keywords[entity]['count'] += 1
#                 entity_keywords[entity]['keywords'].append(row['keyword'])
#             else:
#                 entity_keywords[entity] = {'count': 1, 'keywords': [row['keyword']]}


#     # print (entity_keywords)
#     # with open('entity_keywords.json', 'w') as json_file:
#     #     json.dump(entity_keywords, json_file, indent=4)

#     # with open('entity_keywords.json', 'r') as file:
#     #     data = json.load(file)

#     # # Sort the dictionary items by count in descending order
#     # sorted_keywords = sorted(data.items(), key=lambda x: x[1]['count'], reverse=True)

#     # # Create a new dictionary from the sorted list
#     # sorted_keywords_dict = {keyword: info for keyword, info in sorted_keywords}

#     # # Write the sorted keywords to a new JSON file
#     # with open('entity_keywords.json', 'w') as outfile:
#     #     json.dump(sorted_keywords_dict, outfile, indent=4)

#     # print("Sorted keywords have been stored in entity_keywords.json")

    
#     print(entity_keywords)

#     sorted_keywords = sorted(entity_keywords.items(), key=lambda x: x[1]['count'], reverse=True)

#     sorted_keywords_dict = {keyword: info for keyword, info in sorted_keywords}

#     with open('entity_keywords.json', 'w') as json_file:
#         json.dump(sorted_keywords_dict, json_file, indent=4)

#     print("Sorted keywords have been stored in entity_keywords.json")

# # Run the main function
# asyncio.run(main())


import csv
from pydantic import BaseModel
from typing import List
from llama_index.program.openai import OpenAIPydanticProgram
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import json

class EntitiesInText(BaseModel):
    primary_entities: List[str]

prompt_template_str = """
Role: SEO Specialist. 
Task: List all the primary entities from given text below similar to example below.
Instructions: 
1. First read the whole text and understand what entities it contains and then only give response. 
2. Give only granular primary entities.
3. Exclude all adjectives in the entities
4. Don't use actions in the entities.
5. Understand from examples. Dont just copy them into response.

 -------------------------------
Examples below:
1.  Given text below: "what does best vitamin c serum do for skin"
    output : primary_entities: ["vitamin c serum", "skin"]
2.  Given text below: "best vitamin c serum for mature skin"
    output : primary_entities: ["best vitamin c serum", "mature skin"]
3.  Given text below: "hyper skin brightening dark spot vitamin c serum"
    output : primary_entities: ["hyper skin", "dark spot vitamin","serum"]
4.  Given text below: "benefits of vitamin c serum for the skin"
    output : primary_entities: ["benefits", "vitamin c", "serum", "skin"]
5.  Given text below: "clinical skin vitamin c pro-collagen serumn"
    output : primary_entities: ["clinical skin", "vitamin","pro-collagen serum"]
---------------------------------
Given text below: {keyword}
primary_entities: 
"""

# Split the list into chunks
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# Async function to call the program
async def async_openai_call(program, keyword):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        func = partial(program, keyword=keyword, description="First read the whole text and list all primary entities in the text")
        return await loop.run_in_executor(pool, func)

async def get_keywords_entities(program, keyword, semaphore):
    async with semaphore:
        try:
            output = await async_openai_call(program, keyword)
            if output:
                return {'keyword': keyword, 'entities': output.primary_entities}
        except Exception as e:
            print(f"Error processing keyword '{keyword}': {e}")
            return None

async def process_batch(program, batch, semaphore):
    tasks = [get_keywords_entities(program, keyword, semaphore) for keyword in batch]
    entities_data = await asyncio.gather(*tasks)
    return [data for data in entities_data if data]

async def main():
    csv_file_path = "samaro-keywords.txt"
    keywords = []

    with open(csv_file_path, mode='r') as file:
        keywords = file.readlines()
        keywords = [keyword.strip() for keyword in keywords]

    keywords = keywords[:10000]
    
    program = OpenAIPydanticProgram.from_defaults(output_cls=EntitiesInText, prompt_template_str=prompt_template_str, verbose=False)
    semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent tasks

    all_data = []

    for batch in chunks(keywords, 25):
        data = await process_batch(program, batch, semaphore)
        all_data.extend(data)

    # map entities to keywords
    entity_keywords = {}
    for row in all_data:
        for entity in row['entities']:
            if entity in entity_keywords.keys():
               
                entity_keywords[entity]['count'] += 1
                entity_keywords[entity]['keywords'].append(row['keyword'])
            else:
                entity_keywords[entity] = {'count': 1, 'keywords': [row['keyword']]}

    # Sort the dictionary items by count in descending order
    sorted_keywords = sorted(entity_keywords.items(), key=lambda x: x[1]['count'], reverse=True)

    # Create a new dictionary from the sorted list
    sorted_keywords_dict = {keyword: info for keyword, info in sorted_keywords}

    # Write the sorted keywords to a JSON file
    with open('entity_keywords_samaro.json', 'w') as json_file:
        json.dump(sorted_keywords_dict, json_file, indent=4)

    print("Sorted keywords have been stored in entity_keywords1.json")

# Run the main function
asyncio.run(main())
