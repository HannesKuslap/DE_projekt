# Hangi kõik tööd, mis on viidatud artiklis

import requests

def get_citations(doi):
    base_url = "https://api.crossref.org/works/"
    url = f"{base_url}{doi}"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if 'reference' in data['message']:
            citations = data['message']['reference']
            dois = [citation['DOI'] for citation in citations if 'DOI' in citation]
            return dois
        else:
            return []
    else:
        print(f"Error: Unable to retrieve data. Status code: {response.status_code}")
        return []

# Example usage
paper_doi = "10.1126/science.169.3946.635"
citations = get_citations(paper_doi)

if citations:
    print(f"The paper with DOI {paper_doi} has the following citations:")
    for citation in citations:
        try:
            print(citation['unstructured'])
        except:
            print("UNSTRUCTURED PUUDUB, prindin DOI")
            print(citation['DOI'])
else:
    print(f"No citations found for the paper with DOI {paper_doi}")