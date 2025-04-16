#!/usr/bin/env python3
import sys
from collections import defaultdict
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





# dictionaries for storing from Map-Reducer 1
document_lengths = {}
texts_of_documents = {}
term_occurrences = defaultdict(dict)
vocabulary = set()  # Initialize vocabulary set

def main():
    # insert_document_metadata = session.prepare("INSERT INTO document_metadata (document_id, document_length) VALUES (?, ?)")
    # batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    # batch_size = 0
    # count = 0

    # Initialize data structures
    document_lengths = {}
    term_occurrences = defaultdict(dict)
    texts_of_documents = {}
    vocabulary = set()  # Initialize vocabulary set
    
    for line_of_input in sys.stdin:
        # count +=1
        try:
            line_of_input = line_of_input.strip()
            parts_of_line = line_of_input.split('\t')
            
            if len(parts_of_line) < 2:
                continue
                
            entry_type = parts_of_line[0]
            if entry_type == "DOCUMENT_CONTENT":
                document_id = parts_of_line[1]
                text = parts_of_line[2]
                texts_of_documents[document_id] = text
            elif entry_type == "DOCUMENT_LENGTH":
                document_id = parts_of_line[1]
                document_length = int(parts_of_line[2])
                document_lengths[document_id] = document_length
            elif entry_type == "TERM_OCCURRENCE":
                term = parts_of_line[1]
                document_id = parts_of_line[2]
                frequency = int(parts_of_line[3])
                term_occurrences[term][document_id] = frequency
            elif entry_type == "VOCABULARY":
                term = parts_of_line[1]
                vocabulary.add(term)
                
        except Exception as e:
            sys.stderr.write(f"Error processing line {line_of_input}: {str(e)}\n")
    # batch.add(insert_document_metadata, (str('mapper1_inp'), int(count)))
    # batch_size += 1
    # Execute any remaining items in the batch
    # output to reducer2
    # if batch_size > 0:
    #     session.execute(batch)

    list(map(lambda item: print(f"DOCUMENT_LENGTH\t{item[0]}\t{item[1]}"), document_lengths.items()))

    list(map(lambda term_item: list(map(lambda doc_item: print(f"TERM_OCCURRENCE\t{term_item[0]}\t{doc_item[0]}\t{doc_item[1]}"), term_item[1].items())), term_occurrences.items()))

    list(map(lambda term_item: print(f"TERM_DOCUMENT_FREQ\t{term_item[0]}\t{len(term_item[1])}"), term_occurrences.items()))

    list(map(lambda term: print(f"SEARCH_TERM\t{term}"), term_occurrences.keys()))

    list(map(lambda item: print(f"DOCUMENT_CONTENT\t{item[0]}\t{item[1]}"), texts_of_documents.items()))


    # Output the last term's document frequency
if __name__ == "__main__":
    main()

# process input from mapper1
