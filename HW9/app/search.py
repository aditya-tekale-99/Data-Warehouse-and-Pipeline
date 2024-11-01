import pandas as pd
from vespa.application import Vespa
from vespa.io import VespaResponse, VespaQueryResponse

def display_hits_as_df(response: VespaQueryResponse, fields) -> pd.DataFrame:
    records = []
    for hit in response.hits:
        record = {}
        for field in fields:
            record[field] = hit["fields"][field]
        records.append(record)
    return pd.DataFrame(records)

def keyword_search(app, search_query):
    query = {
        "yql": "select * from sources * where userQuery() limit 5",
        "query": search_query,
        "ranking": "bm25",
    }
    response = app.query(query)
    return display_hits_as_df(response, ["doc_id", "title"])

def semantic_search(app, search_query):
    query = {
        "yql": "select * from sources * where ({targetHits:100}nearestNeighbor(embedding, e)) limit 5",
        "query": search_query,
        "ranking": "semantic",
        "input.query(e)": "embed(@query)"
    }
    response = app.query(query)
    return display_hits_as_df(response, ["doc_id", "title"])

def get_embedding(doc_id):
    query = {
        "yql" : f"select doc_id, title, text, embedding from content.doc where doc_id contains '{doc_id}'",
        "hits": 1
    }
    result = app.query(query)
    
    if result.hits:
        return result.hits[0]
    return None

def query_products_by_embedding(embedding_vector):
    query = {
        'hits': 5,
        'yql': 'select * from content.doc where ({targetHits:5}nearestNeighbor(embedding, user_embedding))',
        'ranking.features.query(user_embedding)': str(embedding_vector),
        'ranking.profile': 'recommendation'
    }
    return app.query(query)

app = Vespa(url="http://localhost", port=8080)

query = "Kindle Paperwhite"

# Perform keyword search
df = keyword_search(app, query)
print("Keyword Search Results:")
print(df.head())

# Perform semantic search
df = semantic_search(app, query)
print("\nSemantic Search Results:")
print(df.head())

# Recommendation based on the product id
doc_id = "AVpe7AsMilAPnD_xQ78G"
emb = get_embedding(doc_id)
if emb:
    results = query_products_by_embedding(emb["fields"]["embedding"])
    df = display_hits_as_df(results, ["doc_id", "title", "text"])
    print("\nRecommendations based on Embedding:")
    print(df.head())
else:
    print(f"No embedding found for document ID: {doc_id}")

