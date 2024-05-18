#!/bin/bash

# Define the path to the .tgz file and the destination directory
SOURCE_FILE="./tolldata.tgz"
DESTINATION_DIR="./extracted_data"
CSV_FILE="$DESTINATION_DIR/vehicle-data.csv"
OUTPUT_CSV="$DESTINATION_DIR/csv_data.csv"
TSV_FILE="$DESTINATION_DIR/tollplaza-data.tsv"
OUTPUT_TSV_CSV="$DESTINATION_DIR/tsv_data.csv"
FIXED_WIDTH_FILE="$DESTINATION_DIR/payment-data.txt"
OUTPUT_FIXED_WIDTH_CSV="$DESTINATION_DIR/fixed_width_data.csv"
CONSOLIDATED_OUTPUT="$DESTINATION_DIR/extracted_data.csv"
TRANSFORMED_OUTPUT="$DESTINATION_DIR/staging/transformed_data.csv"

# Create the destination directory if it does not exist
mkdir -p $DESTINATION_DIR/staging

# Extract the .tgz file to the destination directory
tar -xzvf $SOURCE_FILE -C $DESTINATION_DIR

# Check if the CSV file exists
if [ -f "$CSV_FILE" ]; then
  # Extract specific fields from the CSV file and save to a new CSV file
  # Rowid, Timestamp, Anonymized Vehicle number, Vehicle type are in columns 1, 2, 3, 4 respectively
  awk -F',' 'BEGIN {OFS=","} {print $1, $2, $3, $4}' $CSV_FILE > $OUTPUT_CSV
  echo "Data extracted to $OUTPUT_CSV"
else
  echo "CSV file not found in the extracted data"
fi

# Check if the TSV file exists
if [ -f "$TSV_FILE" ]; then
  # Extract specific fields from the TSV file and save to a new CSV file
  # Number of axles, Tollplaza id, Tollplaza code are in columns 5, 6, 7 respectively
  awk -F'\t' 'BEGIN {OFS=","} {print $5, $6, $7}' $TSV_FILE > $OUTPUT_TSV_CSV
  echo "Data extracted to $OUTPUT_TSV_CSV"
else
  echo "TSV file not found in the extracted data"
fi

# Check if the fixed width file exists
if [ -f "$FIXED_WIDTH_FILE" ]; then
  # Extract specific fields from the fixed-width file and save to a new CSV file
  # Type of Payment code is in columns 55-57 (3 characters)
  # Vehicle Code is in columns 59-64 (6 characters)
  cut -c 55-57,59-64 --output-delimiter=',' $FIXED_WIDTH_FILE > $OUTPUT_FIXED_WIDTH_CSV
  echo "Data extracted to $OUTPUT_FIXED_WIDTH_CSV"
else
  echo "Fixed width file not found in the extracted data"
fi

# Consolidate data from CSV, TSV, and fixed-width files into a single CSV file
paste -d ',' $OUTPUT_CSV $OUTPUT_TSV_CSV $OUTPUT_FIXED_WIDTH_CSV > $CONSOLIDATED_OUTPUT
echo "Data consolidated to $CONSOLIDATED_OUTPUT"

# Transform the vehicle_type field to capital letters and save it to a new file
awk -F',' 'BEGIN {OFS=","} {$4 = toupper($4); print}' $CONSOLIDATED_OUTPUT > $TRANSFORMED_OUTPUT
echo "Data transformed and saved to $TRANSFORMED_OUTPUT"
