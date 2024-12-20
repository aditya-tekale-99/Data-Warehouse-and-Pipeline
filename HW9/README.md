# Amazon Product Review Analysis with VESPA

This project processes Amazon Product Review data, ingests it into VESPA, and performs search queries on the processed dataset. VESPA is run as a Docker container to facilitate big data processing and search functionalities.


## Steps
1. Download the [Amazon Product Reviews dataset](https://www.kaggle.com/datasets/yasserh/amazon-product-reviews-dataset?resource=download) from Kaggle.
2. [Process Amazon product reviews data and generate an input file to VESPA](https://github.com/aditya-tekale-99/Data-Warehouse-and-Pipeline/blob/main/HW9/process_amazon_reviews_2_jsonl.py)
3. Configure VESPA
4. Run VESPA as a Docker Container
5. Ingest the file into VESPA
6. Run [search](https://github.com/aditya-tekale-99/Data-Warehouse-and-Pipeline/blob/main/HW9/search.py) against VESPA
