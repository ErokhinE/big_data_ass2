#!/bin/bash

# Configuration
INPUT_PATH="/index/data"
SPARK_APP_NAME="data_preparation"
HADOOP_MEMORY_MB=2048
HADOOP_JAVA_OPTS="-Xmx1800m"

# Function to log messages with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to process directory with Python and Spark
process_directory() {
    local input_dir="$1"

    log "Processing directory: $input_dir"

    # Activate virtual environment and set Python environment variables
    source .venv/bin/activate
    export PYSPARK_DRIVER_PYTHON=$(which python)
    unset PYSPARK_PYTHON

    # Python script to process files using Spark
    python -c "
import os
import re
from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession

def create_document(row):
    # Create output directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Generate sanitized filename
    filename = 'data/' + sanitize_filename(str(row['id']) + '_' + row['title']).replace(' ', '_') + '.txt'
    
    # Write document content to file
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(row['text'])

# Initialize Spark session
spark = SparkSession.builder.appName('$SPARK_APP_NAME').master('local').getOrCreate()
input_folder = '$input_dir'
documents_data = []

# Process each file in the input directory
for filename in os.listdir(input_folder):
    file_path = os.path.join(input_folder, filename)
    if os.path.isdir(file_path): continue
    
    # Extract document ID and title from filename
    match = re.match(r'(\d+)_(.+)\\.txt$', filename)
    if match:
        doc_id = match.group(1)
        title = match.group(2).replace('_', ' ')
    else:
        doc_id = str(len(documents_data) + 1)
        title = os.path.splitext(filename)[0]
    
    # Read file content
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()
        documents_data.append((doc_id, title, text))
    except Exception as e:
        print(f'Error reading file {filename}: {e}')

# Create DataFrame and process documents
df = spark.createDataFrame(documents_data, ['id', 'title', 'text'])
df.foreach(create_document)
df.write.option('sep', '\t').mode('append').csv('/index/data')
spark.stop()
"

    if [ $? -eq 0 ]; then
        log "Python processing completed successfully."

        # Upload processed files to HDFS
        log "Uploading processed files to HDFS"
        hdfs dfs -put data /
        hdfs dfs -ls /data
        hdfs dfs -ls /index/data

        return 0
    else
        log "Python processing failed!"
        return 1
    fi
}

# Process default data directory
if ! process_directory "/app/data"; then
    log "Failed to process and upload default data directory"
    exit 1
fi

# Handle custom input path if provided
if [ "$#" -eq 1 ]; then
    CUSTOM_INPUT_PATH="$1"

    if [ -e "$CUSTOM_INPUT_PATH" ]; then
        if [ -d "$CUSTOM_INPUT_PATH" ]; then
            log "Processing custom directory..."
            if ! process_directory "$CUSTOM_INPUT_PATH"; then
                log "Failed to process custom directory"
                exit 1
            fi
        elif [ -f "$CUSTOM_INPUT_PATH" ]; then
            log "Uploading single file to HDFS"
            hdfs dfs -put "$CUSTOM_INPUT_PATH" /data/
        else
            log "Unsupported file type, using raw HDFS path"
        fi
    else
        log "Path not found locally, assuming HDFS path"
    fi
fi

# Cleanup previous MapReduce outputs
log "Cleaning up previous MapReduce outputs"
hdfs dfs -rm -r /tmp/index/output1
hdfs dfs -rm -r /tmp/index/output2

log "Starting indexing process from: $INPUT_PATH"

# Make MapReduce scripts executable
log "Setting up MapReduce scripts"
chmod +x $(pwd)/mapreduce/mapper1.py $(pwd)/mapreduce/reducer1.py
chmod +x $(pwd)/mapreduce/mapper2.py $(pwd)/mapreduce/reducer2.py

# First MapReduce job - Document processing
log "Starting First MapReduce Job - Document Processing"
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
    -archives /app/.venv.tar.gz#.venv \
    -D mapreduce.reduce.memory.mb=$HADOOP_MEMORY_MB \
    -D mapreduce.reduce.java.opts=$HADOOP_JAVA_OPTS \
    -mapper ".venv/bin/python mapper1.py" \
    -reducer ".venv/bin/python reducer1.py" \
    -input "$INPUT_PATH" \
    -output /tmp/index/output1

if [ $? -ne 0 ]; then
    log "Error: First MapReduce job failed"
    exit 1
fi

# Second MapReduce job - Index building
log "Starting Second MapReduce Job - Index Building"
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
    -archives /app/.venv.tar.gz#.venv \
    -D mapreduce.reduce.memory.mb=$HADOOP_MEMORY_MB \
    -D mapreduce.reduce.java.opts=$HADOOP_JAVA_OPTS \
    -mapper ".venv/bin/python mapper2.py" \
    -reducer ".venv/bin/python reducer2.py" \
    -input "/tmp/index/output1" \
    -output "/tmp/index/output2"

if [ $? -ne 0 ]; then
    log "Error: Second MapReduce job failed"
    # Get YARN application ID and logs for debugging
    APP_ID=$(yarn application -list | grep "application_" | tail -1 | awk '{print $1}')
    if [ ! -z "$APP_ID" ]; then
        log "YARN Application ID: $APP_ID"
        log "Error Log Snippet:"
        yarn logs -applicationId "$APP_ID" | grep -A 20 "stderr"
    fi
    exit 1
fi

log "Indexing completed successfully!"
log "Index data is now stored in Cassandra."

# Final cleanup
log "Performing final cleanup"
hdfs dfs -rm -r -f /tmp/index/output1
hdfs dfs -rm -r -f /tmp/index/output2
