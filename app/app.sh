#!/bin/bash

# Configuration
CASSANDRA_HOST="cassandra-server"
CASSANDRA_INIT_FILE="init_cassandra.cql"
CASSANDRA_CHECK_INTERVAL=5
SEARCH_QUERY="Summer beach vibes"

# Function to log messages with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to check if Cassandra is ready
wait_for_cassandra() {
    log "Waiting for Cassandra to be ready..."
    until cqlsh $CASSANDRA_HOST -e "describe keyspaces" > /dev/null 2>&1; do
        log "Cassandra is unavailable - sleeping"
        sleep $CASSANDRA_CHECK_INTERVAL
    done
    log "Cassandra is up - continuing"
}

# Function to initialize Cassandra schema
initialize_cassandra() {
    log "Initializing Cassandra schema..."
    cqlsh $CASSANDRA_HOST -f $CASSANDRA_INIT_FILE
    if [ $? -eq 0 ]; then
        log "Cassandra schema initialized successfully"
    else
        log "Failed to initialize Cassandra schema"
        exit 1
    fi
}

# Function to setup Python environment
setup_python_env() {
    log "Setting up Python environment..."
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    venv-pack -o .venv.tar.gz
}

# Main execution
main() {
    # Start SSH server
    log "Starting SSH server..."
    service ssh restart

    # Start required services
    log "Starting services..."
    bash start-services.sh

    # Setup Python environment
    setup_python_env

    # Wait for Cassandra and initialize schema
    wait_for_cassandra
    initialize_cassandra

    # Process data and build index
    log "Preparing data..."
    bash prepare_data.sh

    log "Building index..."
    bash index.sh

    # Run search query
    log "Running search query: $SEARCH_QUERY"
    bash search.sh "$SEARCH_QUERY"
}

# Execute main function
main