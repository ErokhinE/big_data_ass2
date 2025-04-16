#!/usr/bin/env python3

import sys
import math
import time
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import re

# Configuration
CASSANDRA_HOST = 'cassandra-server'
CASSANDRA_KEYSPACE = 'search_engine'
SPARK_APP_NAME = 'DocumentSearchEngine'
MAX_RETRIES = 5
RETRY_INTERVAL = 5
BM25_K1 = 1.2  # Term frequency saturation parameter
BM25_B = 0.75  # Document length normalization parameter
BM25_K3 = 1.2  # Query term frequency parameter
TOP_RESULTS = 10
NLTK_DOWNLOADS = ['stopwords', 'punkt', 'punkt_tab']
STOPWORDS_LANGUAGE = 'english'

# Initialize NLTK components
for package in NLTK_DOWNLOADS:
    nltk.download(package, quiet=True)
stop_words = set(stopwords.words(STOPWORDS_LANGUAGE))
stemmer = PorterStemmer()

class SearchEngine:
    def __init__(self):
        """Initialize the search engine with Cassandra and Spark connections"""
        self.spark = self._initialize_spark()
        self.cluster, self.session = self._connect_to_cassandra()
        
    def _initialize_spark(self):
        """Initialize Spark session"""
        return SparkSession.builder \
            .appName(SPARK_APP_NAME) \
            .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
            .getOrCreate()
            
    def _connect_to_cassandra(self, max_retries=MAX_RETRIES, retry_interval=RETRY_INTERVAL):
        """Establish connection to Cassandra with retry mechanism"""
        for attempt in range(max_retries):
            try:
                cluster = Cluster([CASSANDRA_HOST])
                session = cluster.connect()
                # Verify connection by checking keyspace
                session.execute(f"USE {CASSANDRA_KEYSPACE}")
                return cluster, session
            except Exception as e:
                if attempt < max_retries - 1:
                    sys.stderr.write(f"Cassandra connection attempt {attempt + 1} failed: {str(e)}\n")
                    time.sleep(retry_interval)
                else:
                    sys.stderr.write(f"All Cassandra connection attempts failed: {str(e)}\n")
                    raise
        
    def _fetch_table_data(self, table_name):
        """Fetch all rows from a Cassandra table"""
        try:
            rows = self.session.execute(f"SELECT * FROM {table_name}")
            return list(rows)
        except Exception as e:
            sys.stderr.write(f"Error fetching data from {table_name}: {str(e)}\n")
            return []
        
    def _process_search_query(self, query_text):
        """Process and tokenize the search query"""
        # Clean and normalize text
        text = re.sub(r'[^a-zA-Z0-9\s]', ' ', query_text.lower())
        
        # Tokenize
        try:
            tokens = word_tokenize(text)
        except Exception:
            tokens = text.split()
            
        # Remove stopwords and stem
        return [stemmer.stem(token) for token in tokens 
                if token.isalnum() and token not in stop_words]
                
    def _calculate_bm25_score(self, query_terms, document_id, document_length, 
                            avg_document_length, term_occurrences, 
                            term_document_freqs, total_documents):
        """Calculate BM25 score for a document using the standard formula"""
        score = 0.0
        for search_term in query_terms:
            if (search_term in term_occurrences and 
                document_id in term_occurrences[search_term]):
                term_count = term_occurrences[search_term][document_id]
                document_count = term_document_freqs.get(search_term, 0)
                
                if document_count > 0:
                    # Calculate IDF component
                    idf = math.log((total_documents - document_count + 0.5) / 
                                 (document_count + 0.5) + 1.0)
                    
                    # Calculate TF component
                    tf_numerator = term_count * (BM25_K1 + 1)
                    tf_denominator = (term_count + BM25_K1 * 
                                    (1 - BM25_B + BM25_B * document_length / avg_document_length))
                    
                    # Calculate query term frequency component
                    query_term_freq = query_terms.count(search_term)
                    query_component = (query_term_freq * (BM25_K3 + 1)) / (query_term_freq + BM25_K3)
                    
                    score += idf * (tf_numerator / tf_denominator) * query_component
        return score
        
    def search(self, query_text):
        """Execute search query and return results"""
        # Process query
        search_terms = self._process_search_query(query_text)
        if not search_terms:
            return "Empty query. Please provide search terms."
            
        try:
            # Fetch required data from Cassandra
            document_metadata = {
                row.document_id: row.document_length 
                for row in self._fetch_table_data('document_metadata')
            }
            
            term_occurrences = {}
            for row in self._fetch_table_data('term_occurrences'):
                if row.search_term not in term_occurrences:
                    term_occurrences[row.search_term] = {}
                term_occurrences[row.search_term][row.document_id] = row.term_count
                
            term_document_freqs = {
                row.search_term: row.document_count 
                for row in self._fetch_table_data('term_document_frequency')
            }
            
            document_contents = {
                row.document_id: row.document_text 
                for row in self._fetch_table_data('document_content')
            }
            
            # Calculate average document length
            total_documents = len(document_metadata)
            if total_documents == 0:
                return "No documents in the database."
                
            avg_document_length = sum(document_metadata.values()) / total_documents
            
            # Calculate scores for each document
            scores = {}
            for document_id in document_metadata:
                score = self._calculate_bm25_score(
                    search_terms, document_id, document_metadata[document_id],
                    avg_document_length, term_occurrences, term_document_freqs,
                    total_documents
                )
                scores[document_id] = score
            
            # Sort and return top results
            sorted_results = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            results = []
            
            for document_id, score in sorted_results[:TOP_RESULTS]:
                results.append(f"Document {document_id} (Score: {score:.4f})")
                
            return "\n".join(results) if results else "No matching documents found."
            
        except Exception as e:
            return f"Error during search: {str(e)}"
            
    def __del__(self):
        """Clean up resources"""
        if hasattr(self, 'cluster'):
            self.cluster.shutdown()
        if hasattr(self, 'spark'):
            self.spark.stop()

def main():
    print(" ".join(sys.argv[1:]))
    if len(sys.argv) < 2:
        print("Usage: python query.py <search_query>")
        sys.exit(1)
        
    search_engine = SearchEngine()
    print(" ".join(sys.argv[1:]))
    results = search_engine.search(" ".join(sys.argv[1:]))
    print(results)

if __name__ == "__main__":
    main()