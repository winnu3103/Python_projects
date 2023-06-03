import os
import json
import csv

def flatten_json(data):
    flattened_data = {}

    def flatten(value, key=""):
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flatten(sub_value, key + sub_key + "_")
        elif isinstance(value, list):
            for index, sub_value in enumerate(value):
                flatten(sub_value, key + str(index) + "_")
        else:
            flattened_data[key[:-1]] = value

    flatten(data)
    return flattened_data


data_dir = "data"
json_files = []

# Find all JSON files in the data directory
for root, dirs, files in os.walk(data_dir):
    for file in files:
        if file.endswith(".json"):
            json_files.append(os.path.join(root, file))

# Process each JSON file
for json_file in json_files:
    with open(json_file) as f:
        json_data = json.load(f)

    flattened_data = flatten_json(json_data)

    csv_file = os.path.splitext(json_file)[0] + ".csv"

    with open(csv_file, "w", newline="") as f:
        fieldnames = flattened_data.keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(flattened_data)
