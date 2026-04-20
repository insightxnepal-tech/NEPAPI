import json
import csv

def main():
    print("Loading JSON...")
    with open('floorsheet.json', 'r') as f:
        data = json.load(f)
    print("Writing CSV...")
    if data:
        with open('floorsheet.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
    print("Conversion complete.")

main()
