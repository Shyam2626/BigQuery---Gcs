import csv

def convert_plain_text_to_csv(plain_text_file, csv_output_file):
    # Open the plain text file for reading
    with open(plain_text_file, 'r') as text_file:
        lines = text_file.readlines()

    # Open the CSV file for writing
    with open(csv_output_file, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)

        # Example: Assuming lines are comma-separated in the text file
        for line in lines:
            # Split the plain text line into columns (assuming comma as delimiter)
            row = line.strip().split(',')  # Change this if your delimiter is different
            writer.writerow(row)

# Usage: Convert plain text to CSV
plain_text_file = '/home/shyam/Downloads/time-testing'
csv_output_file = '/home/shyam/Downloads/output.csv'
convert_plain_text_to_csv(plain_text_file, csv_output_file)
