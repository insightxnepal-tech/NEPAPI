
import asyncio
from nepse import AsyncNepse
import json
import csv

async def fetch_and_save():
    print("Initializing AsyncNepse...")
    nepseAsync = AsyncNepse()
    nepseAsync.setTLSVerification(False)
    print("Fetching floorsheet data...")
    data = await nepseAsync.getFloorSheet()
    print(f"Got {len(data)} records.")
    with open("floorsheet.json", "w") as f:
        json.dump(data, f, indent=4)
    print("Saved to floorsheet.json")

    if data:
        with open("floorsheet.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print("Saved to floorsheet.csv")

try:
    asyncio.run(fetch_and_save())
except Exception as e:
    print(f"Error fetching floorsheet: {e}")
