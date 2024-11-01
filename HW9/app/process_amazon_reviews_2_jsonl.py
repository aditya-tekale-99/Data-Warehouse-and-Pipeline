#importing python libraries
import pandas as pd
import json

# function to combine relevant text features into single text field
def combine_features(row):
    try:
        return f"{row['reviews.title']} {row['reviews.text']} {row['categories']}"
    except Exception as e:
        print("Error:", e)
        return ""

# function to process the csv file to jsonl
def process_amazon_reviews_csv(input_file, output_file):
    reviews = pd.read_csv(input_file)
    # Fill NaN values in relevant columns
    for column in ['reviews.title', 'reviews.text', 'categories']:
        reviews[column] = reviews[column].fillna('')

    # Create a 'text' column for Vespa using the combined features
    reviews["text"] = reviews.apply(combine_features, axis=1)
    
    # Select and rename columns for Vespa compatibility
    reviews = reviews[['id', 'name', 'text']]
    reviews.rename(columns={'id': 'doc_id', 'name': 'title'}, inplace=True)

    # Create 'fields' column with JSON-like structure for each record
    reviews['fields'] = reviews.apply(lambda row: row.to_dict(), axis=1)

    # Create 'put' column based on 'doc_id' to uniquely identify each document
    reviews['put'] = reviews['doc_id'].apply(lambda x: f"id:amazon-reviews:doc::{x}")

    # Save to JSONL file
    df_result = reviews[['put', 'fields']]
    df_result.to_json(output_file, orient='records', lines=True)

# executing the function with defined parameters
process_amazon_reviews_csv("amazon_product_reviews.csv", "amazon_reviews.jsonl")
