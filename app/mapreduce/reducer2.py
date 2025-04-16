#!/usr/bin/env python3
import sys
import time
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

# Configuration
CASSANDRA_HOST = 'cassandra-server'
CASSANDRA_KEYSPACE = 'search_engine'
MAX_RETRIES = 5
RETRY_INTERVAL = 5
MAX_BATCH_SIZE = 1  # Increased batch size for better performance

def cassandra_connection(max_retries=MAX_RETRIES, retry_interval=RETRY_INTERVAL):
    """Establish connection to Cassandra with retry mechanism"""
    for attempt in range(max_retries):
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect()
            return cluster, session
        except Exception as e:
            if attempt < max_retries - 1:
                sys.stderr.write(f"Cassandra connection attempt {attempt + 1} failed: {str(e)}\n")
                time.sleep(retry_interval)
                exit(1)
            else:
                sys.stderr.write(f"All Cassandra connection attempts failed: {str(e)}\n")
                exit(1)

def main():
    try:
        # Connect to Cassandra
        cluster, session = cassandra_connection()
        
        # Use the search engine keyspace
        session.execute(f"USE {CASSANDRA_KEYSPACE}")
        count = 0
    
        
        # Prepare insert statements for each table
        insert_document_metadata = session.prepare("INSERT INTO document_metadata (document_id, document_length) VALUES (?, ?)")
        insert_search_term = session.prepare("INSERT INTO search_terms (search_term) VALUES (?)")
        insert_document_content = session.prepare("INSERT INTO document_content (document_id, document_text) VALUES (?, ?)")
        insert_term_occurrence = session.prepare("INSERT INTO term_occurrences (search_term, document_id, term_count) VALUES (?, ?, ?)")
        insert_term_document_freq = session.prepare("INSERT INTO term_document_frequency (search_term, document_count) VALUES (?, ?)")
        
        # Initialize batch processing
        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        batch_size = 0
        
        # Process input from mapper2
        for line in sys.stdin:
            count +=1
            try:
                line = line.strip()
                if not line:
                    continue
                    
                line_parts = line.split('\t')
                if len(line_parts) < 2:
                    continue
                    
                entry_type = line_parts[0]
                
                if entry_type == "TERM_OCCURRENCE":
                    search_term = line_parts[1]
                    document_id = line_parts[2]
                    term_count = int(line_parts[3])
                    batch.add(insert_term_occurrence, (str(search_term), str(document_id), int(term_count)))
                    batch_size += 1
                
                elif entry_type == "DOCUMENT_LENGTH":
                    document_id = line_parts[1]
                    document_length = int(line_parts[2])
                    batch.add(insert_document_metadata, (str(document_id), int(document_length)))
                    batch_size += 1
                    
        
                    
                elif entry_type == "TERM_DOCUMENT_FREQ":
                    search_term = line_parts[1]
                    document_count = int(line_parts[2])
                    batch.add(insert_term_document_freq, (str(search_term), int(document_count)))
                    batch_size += 1
                    
              
                elif entry_type == "DOCUMENT_CONTENT":
                    document_id = line_parts[1]
                    document_text = line_parts[2]
                    batch.add(insert_document_content, (str(document_id), str(document_text)))
                    batch_size += 1

                elif entry_type == "SEARCH_TERM":
                    search_term = line_parts[1]
                    batch.add(insert_search_term, (str(search_term),))
                    batch_size += 1
                
                # Execute batch when it reaches maximum size
                if batch_size >= MAX_BATCH_SIZE:
                    session.execute(batch)
                    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                    batch_size = 0
                    
            except Exception as e:
                sys.stderr.write(f"Error in processing: {str(e)}\n")

        batch.add(insert_document_metadata, (str('abc'), int(count)))
        batch_size += 1
        # Execute any remaining items in the batch
        if batch_size > 0:
            session.execute(batch)
        
    except Exception as e:
        sys.stderr.write(f"R2 Error: {str(e)}\n")
        sys.exit(1)
        
    finally:
        # Close the Cassandra connection
        if 'cluster' in locals():
            cluster.shutdown()

if __name__ == "__main__":
    main()