import asyncio
import csv
from nepse import AsyncNepse
import tqdm.asyncio
from datetime import date

async def fetch_floorsheet():
    print("Initializing AsyncNepse...")
    nepseAsync = AsyncNepse()
    nepseAsync.setTLSVerification(False)
    
    print("Fetching today's floorsheet...")
    # getFloorSheet() fetches the latest session's data
    final_data = await nepseAsync.getFloorSheet(show_progress=True)
    
    if not final_data:
        print("No floorsheet data found.")
        return []
        
    print(f"Got {len(final_data)} records.")
    return final_data

async def main():
    today_str = date.today().strftime("%Y-%m-%d")
    data = await fetch_floorsheet()
    
    if data:
        csv_filename = f"floorsheet_{today_str}.csv"
        print(f"Saving to {csv_filename}...")
        with open(csv_filename, "w", newline='') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print(f"Done! Data saved to {csv_filename}")
        
        # Also save a copy as floorsheet.csv for convenience
        with open("floorsheet.csv", "w", newline='') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print("Updated floorsheet.csv")
    else:
        print("No data to save.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Error: {e}")
