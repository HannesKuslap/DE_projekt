import json
import pandas as pd

def read_json(line):
    one_article = json.loads(line)
    return one_article

def flatten(target):
    result = []
    for entry in target:
        full_name = f"{entry[0]}, {entry[1]}"
        result.append(full_name.strip())
    return result

# Reads a jsonfile, creates a csv 
def transform_data(jsonfile, output_csv):
    data = pd.DataFrame(columns=('id', 'authors', 'title', 'journal-ref', 'doi', 'categories', 'date_created'))
    with open(jsonfile, "r", encoding='utf-8') as json_file:
        n = 0
        for line in json_file:
            one_article = read_json(line)
            if one_article['doi'] == None: # vb ei peaks ikka nii tegema aga praegu viskab välja kõik millel pole doi-d
                continue

            authors = flatten(one_article['authors_parsed'])
            # formaat ["perekonnanimi, eesnimi", "perekonnanimi, eesnimi", ...]
            data.loc[n] = [one_article['id'], authors, one_article['title'],
                        one_article['journal-ref'], one_article['doi'], one_article['categories'].split(" "), 
                        one_article['versions'][0]['created']]
            n += 1
            #if n == 10000:
                #return data
    data.to_csv(output_csv, index=False)
