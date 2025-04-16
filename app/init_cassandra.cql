-- Initialize document search engine keyspace and tables
CREATE KEYSPACE IF NOT EXISTS search_engine
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

USE search_engine;

-- Table for storing unique search terms
CREATE TABLE IF NOT EXISTS search_terms (
    search_term text PRIMARY KEY
);

-- Table for storing document metadata
CREATE TABLE IF NOT EXISTS document_metadata (
    document_id text PRIMARY KEY,
    document_length int
);

-- Table for storing term occurrences in documents
CREATE TABLE IF NOT EXISTS term_occurrences (
    search_term text,
    document_id text,
    term_count int,
    PRIMARY KEY (search_term, document_id)
);

-- Table for storing document frequency of terms
CREATE TABLE IF NOT EXISTS term_document_frequency (
    search_term text PRIMARY KEY,
    document_count int
);

-- Table for storing document content
CREATE TABLE IF NOT EXISTS document_content (
    document_id text PRIMARY KEY,
    document_text text
); 