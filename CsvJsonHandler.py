import json
import os
import shutil

import pandas as pd
from tinydb import TinyDB


def convert_csv_to_json(destination_dir, source_dir, csv_origin_file, json_destination_file):
    # Replace with your CSV file path
    csv_origin_file = os.path.join(destination_dir, csv_origin_file)
    df = pd.read_csv(csv_origin_file)

    # Convert the DataFrame to JSON format
    json_data = df.to_json(orient='records')

    # Save the JSON data to a file

    json_destintion_file = json_destination_file + ".json"  # Replace with the desired output JSON file path

    json_file = os.path.join(destination_dir, json_destination_file)
    db = TinyDB(json_file)
    data = json.loads(json_data)
    table = db.table('_default')
    table.insert_multiple(data)
    # with open(json_file, 'w') as f:
    #    f.write(json_data)
    print(f"CSV data converted and saved to {json_file} in JSON format.")
    return json_file


def move_file_to_folder(source_file, destination_folder):
    # Create the destination folder if it doesn't exist
    os.makedirs(destination_folder, exist_ok=True)

    # Get the filename from the source file path
    filename = os.path.basename(source_file)

    # Construct the destination path
    destination_path = os.path.join(destination_folder, filename)

    # Move the file to the destination folder
    shutil.move(source_file, destination_path)
    print(f"File '{filename}' moved to '{destination_folder}'")
    pass
