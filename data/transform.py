import pandas as pd
import json
from datetime import datetime

def read_json(line):
    one_article = json.loads(line)
    return one_article

def flatten(target):
    result = []
    for entry in target:
        full_name = f"{entry[0]}, {entry[1]}"
        result.append(full_name.strip())
    return result

def convert_to_iso_date(datetime_string):
    if pd.notna(datetime_string):  # Check if the value is not None/NaN
        try:
            # Try to parse as '2009-06-23'
            parsed_date = datetime.strptime(datetime_string, '%Y-%m-%d')
        except ValueError:
            # If parsing as '2009-06-23' fails, try the original format
            parsed_date = datetime.strptime(datetime_string, "%a, %d %b %Y %H:%M:%S %Z")

        return parsed_date.strftime('%Y-%m-%d')
    else:
        return None
    
def load_data(jsonfile):
    with open(jsonfile, "r", encoding='utf-8') as json_file:
        ids, titles, authors, journal_refs, categories, dois, creations, updates = [], [], [], [], [], [], [], []
        
        file = json.load(json_file)
        n = 0
        for one_article in file: 
            ids.append(one_article['id'])
            titles.append(one_article['title'])
            authors.append(flatten(one_article['authors_parsed']))
            journal_refs.append(one_article['journal-ref'])
            # Leave only the first category and DOI code
            categories.append(one_article['categories'].split(" ")[0])
            dois.append(one_article['doi'].split(" ")[0])
            # Unify the datetimes
            creations.append(convert_to_iso_date(one_article['versions'][0]['created']))
            updates.append(convert_to_iso_date(one_article['update_date']))
            
            n+=1

    data = {'id': ids, 'authors': authors, 'title': titles, 'journal_ref': journal_refs, 'doi': dois, 'category_main': categories, 'date_created': creations, 'date_updated': updates}
    return pd.DataFrame.from_dict(data)

# Main function to load, wrangle and transform the data into a csv file. 
def transform_data(jsonfile):
    dataframe = load_data(jsonfile)

    # Remove articles where title is only one word. 
    short_title_mask = dataframe['title'].apply(lambda x: len(x.split()) > 1)
    dataframe = dataframe[short_title_mask]

    csv_file_path = jsonfile.split(".")[0] + '.csv'
    dataframe.to_csv(csv_file_path, index=False)

    return csv_file_path # Kas siin peaks olema kaustaviide ka juures?
