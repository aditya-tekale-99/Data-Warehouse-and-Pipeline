import pandas as pd
import json

def combine_features(row):
    """Combine relevant text features into a single text field."""
    try:
        return f"{row['reviews.title']} {row['reviews.text']} {row['categories']}"
    except Exception as e:
        print("Error:", e)
        return ""

def process_amazon_reviews_csv(input_file, output_file):
    reviews = pd.read_csv(input_file)
    # Fill NaN values in relevant columns
    for column in ['reviews.title', 'reviews.text', 'categories']:
        reviews[column] = reviews[column].fillna('')

    # Create a 'text' column for Vespa using the combined features
    reviews["text"] = reviews.apply(combine_features, axis=1)
    
    # Select and rename columns for Vespa compatibility
    reviews = reviews[['id', 'brand', 'text']]
    reviews.rename(columns={'id': 'doc_id', 'brand': 'title'}, inplace=True)

    # Create 'fields' column with JSON-like structure for each record
    reviews['fields'] = reviews.apply(lambda row: row.to_dict(), axis=1)

    # Create 'put' column based on 'doc_id' to uniquely identify each document
    reviews['put'] = reviews['doc_id'].apply(lambda x: f"id:amazon-reviews:doc::{x}")

    # Save to JSONL file
    df_result = reviews[['put', 'fields']]
    df_result.to_json(output_file, orient='records', lines=True)

# Example usage:
process_amazon_reviews_csv("amazon_product_reviews.csv", "amazon_reviews.jsonl")

