#!/usr/bin/env python3
import sys
import re
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import nltk
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

# Configuration
# CASSANDRA_HOST = 'cassandra-server'
# CASSANDRA_KEYSPACE = 'search_engine'
# MAX_RETRIES = 5
# RETRY_INTERVAL = 5
# MAX_BATCH_SIZE = 1  # Increased batch size for better performance

# def cassandra_connection(max_retries=MAX_RETRIES, retry_interval=RETRY_INTERVAL):
#     """Establish connection to Cassandra with retry mechanism"""
#     for attempt in range(max_retries):
#         try:
#             cluster = Cluster([CASSANDRA_HOST])
#             session = cluster.connect()
#             return cluster, session
#         except Exception as e:
#             if attempt < max_retries - 1:
#                 sys.stderr.write(f"Cassandra connection attempt {attempt + 1} failed: {str(e)}\n")
#                 exit(1)
#             else:
#                 sys.stderr.write(f"All Cassandra connection attempts failed: {str(e)}\n")
#                 exit(1)
# cluster, session = cassandra_connection()
        
# # Use the search engine keyspace
# session.execute(f"USE {CASSANDRA_KEYSPACE}")



# Prepare insert statements for each table

# C:\Users\erzhe\big_data_assignmet_2-main\app\mapreduce\mapper1.py

# Configuration
NLTK_DOWNLOADS = ['stopwords', 'punkt','punkt_tab']
STOPWORDS_LANGUAGE = 'english'

# Download required NLTK data
for package in NLTK_DOWNLOADS:
    nltk.download(package, quiet=True)

# Initialize NLTK components
stop_words = set(stopwords.words(STOPWORDS_LANGUAGE))
stemmer = PorterStemmer()

def process_document(document_text):
    """Process document text to extract search terms"""
    # Convert to lowercase and remove special characters
    text = re.sub(r'[^a-zA-Z0-9\s]', ' ', document_text.lower())
    tokens = word_tokenize(text)
    
    # Remove stopwords and stem
    processed_terms = []
    for tokenized_text in tokens:
        if tokenized_text.isalnum() and tokenized_text not in stop_words:
            processed_terms.append(stemmer.stem(tokenized_text))
    
    return processed_terms

def main():
    # insert_document_metadata = session.prepare("INSERT INTO document_metadata (document_id, document_length) VALUES (?, ?)")
    # batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    # batch_size = 0
    # count = 0
    for line in sys.stdin:
        # count +=1
        try:
            # Parse input line
            parts_of_line = line.strip().split('\t')
            if len(parts_of_line) < 2:
                continue
                
            document_id = parts_of_line[0]
            title_of_document = parts_of_line[1]
            document_text = parts_of_line[2]
            
            # Process document text
            search_terms = process_document(document_text)
            
            # Calculate document length
            document_length = len(search_terms)
            
            # Output document metadata
            print(f"DOCUMENT_LENGTH\t{document_id}\t{document_length}")
            
            # Output document content
            print(f"DOCUMENT_CONTENT\t{document_id}\t{document_text}")
            
            # Count term occurrences
            term_counts = {}
            term_counts.update({term: term_counts.get(term, 0) + 1 for term in search_terms})
            
            # Output term occurrences and search terms
            for search_term, term_count in term_counts.items():
                print(f"TERM_OCCURRENCE\t{search_term}\t{document_id}\t{term_count}")
                
        except Exception as e:
            sys.stderr.write(f"Error in document {document_id}: {str(e)}\n")
    # batch.add(insert_document_metadata, (str('mapper1_inp'), int(count)))
    # batch_size += 1
    # Execute any remaining items in the batch
    # if batch_size > 0:
    #     session.execute(batch)

if __name__ == "__main__":
    main()