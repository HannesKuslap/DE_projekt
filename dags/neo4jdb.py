from py2neo import Graph, Node, Relationship
import json
import os

class Neo4jGraph:
    def __init__(self, uri, auth):
        self.graph = Graph(uri, auth=auth)

    def create_author_node(self, author_data):
        author_name = f"{author_data[0]} {author_data[1]} {author_data[2]}"
        
        # Check if the author node already exists
        existing_author_node = self.graph.run(
            "MERGE (a:Author {name: $name}) RETURN a",
            name=author_name
        ).evaluate()
    
        return existing_author_node
 
    def create_article_node(self, article_data):
        article_node = Node("Article",
                            title=article_data['title'],
                            submitter=article_data['submitter'],
                            doi=article_data['doi'])
        self.graph.create(article_node)
        return article_node

    def create_written_by_relationship(self, author_node, article_node):
        written_by = Relationship(author_node, "WRITTEN_BY", article_node)
        self.graph.create(written_by)

    def create_references_relationships(self, source_node, references):
        for target_doi in references:
            self.graph.run(
                "MATCH (source:Article {doi: $source_doi}), "
                "(target:Article {doi: $target_doi}) "
                "MERGE (source)-[:REFERENCES]->(target)",
                source_doi=source_node['doi'], target_doi=target_doi
            )
    
    def get_coauthors(self, author_name):
        query = (
            f"MATCH (author:Author {{name: '{author_name}'}})"
            "MATCH (author)-[:WRITTEN_BY]->(article)<-[:WRITTEN_BY]-(coAuthor)"
            "WHERE coAuthor <> author"
            "RETURN DISTINCT coAuthor.name AS coAuthorName"
        )
        return self.graph.run(query).data()

    def calculate_coauthorship_score(self, author_name):
        query = (
            f"MATCH (author:Author {{name: '{author_name}'}})-[:WRITTEN_BY]->(article)<-[:WRITTEN_BY]-(coAuthor:Author)-[:WRITTEN_BY]->(otherArticle)"
            "RETURN coAuthor.name AS coAuthorName, COUNT(DISTINCT otherArticle) AS coAuthorshipScore"
            "ORDER BY coAuthorshipScore DESC"
        )
        return self.graph.run(query).data()

    def community_analysis(self):
        query = """
        CALL algo.louvain(
          'MATCH (a:Author)-[:WRITTEN_BY]->(ar:Article) RETURN id(a) as id, id(ar) as communityId',
          'MATCH (a1:Author)-[:WRITTEN_BY]->(ar1:Article)<-[:REFERENCES]-(ar2:Article)<-[:WRITTEN_BY]-(a2:Author)
           RETURN id(a1) as source, id(a2) as target, COUNT(DISTINCT ar1) + COUNT(DISTINCT ar2) as weight',
          {graph: 'cypher', write: true, writeProperty: 'communityId'}
        )
        """
        self.graph.run(query)

def process_json_file(file_path, neo4j_graph):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # Process the data and create nodes and relationships
    for i in range(len(data)):
        author_names = data[i]['authors_parsed']
        author_nodes = [neo4j_graph.create_author_node(name) for name in author_names]

        article_node = neo4j_graph.create_article_node(data[i])

        for author_node in author_nodes:
            neo4j_graph.create_written_by_relationship(author_node, article_node)

    # Creating reference links after all article nodes are in the database
    # Json file shoud have field called "references":[doi, doi, doi]
    for i in range(len(data)):
        references = data[i].get('references')
        if references:
            article_node = neo4j_graph.graph.run(
                "MATCH (a:Article {doi: $doi}) RETURN a",
                doi=data[i]['doi']
            ).evaluate()

            neo4j_graph.create_references_relationships(article_node, references)

def main():
    data_folder = 'data'

    # Create Neo4jGraph instance
    neo4j_graph = Neo4jGraph(uri="bolt://localhost:7687", auth=("neo4j", "Lammas123"))

    # Process each JSON file in the data folder, change the number to load more json files
    for file_number in range(1, 2):
        file_path = os.path.join(data_folder, f'arxiv_subset_{file_number}.json')
        process_json_file(file_path, neo4j_graph)


if __name__ == '__main__':
    main()