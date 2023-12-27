from py2neo import Graph, Node, Relationship

class Neo4jGraph:
    def __init__(self, uri, auth):
        self.graph = Graph(uri, auth=auth)

    def create_author_node(self, author_name):
        author_node = Node("Author", name=author_name)
        self.graph.create(author_node)
        return author_node
 
    def create_article_node(self, article_data):
        article_node = Node("Article",
                            title=article_data['title'],
                            abstract=article_data['abstract'],
                            submitter=article_data['submitter'],
                            doi=article_data['doi'],
                            update_date=article_data['update_date'])
        self.graph.create(article_node)
        return article_node

    def create_written_by_relationship(self, author_node, article_node):
        written_by = Relationship(author_node, "WRITTEN_BY", article_node)
        self.graph.create(written_by)

def main():
    arxiv_entry = {
        "id": "0704.0001",
        "submitter": "Pavel Nadolsky",
        "authors": "C. Bal\'azs, E. L. Berger, P. M. Nadolsky, C.-P. Yuan",
        "title": "Calculation of prompt diphoton production cross sections at Tevatron and LHC energies",
        "doi": "10.1103/PhysRevD.76.013009",
        "update_date": "2008-11-26",
        "abstract": "A fully differential calculation in perturbative quantum chromodynamics is presented...",
    }
 
    # Create Neo4jGraph instance
    neo4j_graph = Neo4jGraph(uri="bolt://localhost:7687", auth=("neo4j", "Lammas123"))
    
    # Process the data and create nodes and relationships
    author_names = arxiv_entry['authors'].split(', ')
    author_nodes = [neo4j_graph.create_author_node(name) for name in author_names]
    
    article_node = neo4j_graph.create_article_node(arxiv_entry)
    
    for author_node in author_nodes:
        neo4j_graph.create_written_by_relationship(author_node, article_node)

if __name__ == '__main__':
    main()