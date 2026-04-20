import asyncio
import json
import csv
from nepse import AsyncNepse
import tqdm.asyncio

async def fetch_historical_floorsheet(business_date):
    print(f"Initializing AsyncNepse for date {business_date}...")
    nepseAsync = AsyncNepse()
    nepseAsync.setTLSVerification(False)
    
    # Manually construct the URL with businessDate
    url = f"{nepseAsync.api_end_points['floor_sheet']}?&businessDate={business_date}&size={nepseAsync.floor_sheet_size}&sort=contractId,desc"
    
    print("Fetching first page...")
    sheet = await nepseAsync.requestPOSTAPI(
        url=url, payload_generator=nepseAsync.getPOSTPayloadIDForFloorSheet
    )
    
    if not sheet or 'floorsheets' not in sheet:
        print("No floorsheet data found for this date.")
        return []
        
    floor_sheets = sheet["floorsheets"]["content"]
    max_page = sheet["floorsheets"]["totalPages"]
    
    print(f"Total pages to fetch: {max_page}")
    page_range = range(1, max_page)
    
    awaitables = map(
        lambda page_number: nepseAsync._getFloorSheetPageNumber(
            url,
            page_number,
        ),
        page_range,
    )
    
    print("Fetching remaining pages...")
    remaining_floor_sheets = await tqdm.asyncio.tqdm.gather(*awaitables)
    
    floor_sheets_all = [floor_sheets] + list(remaining_floor_sheets)
    final_data = [row for array in floor_sheets_all for row in array]
    
    print(f"Got {len(final_data)} records for {business_date}.")
    return final_data

async def fetch_and_save():
    date_to_fetch = "2026-03-25"
    data = await fetch_historical_floorsheet(date_to_fetch)
    
    if data:
        print("Saving to JSON...")
        with open("floorsheet_2026_03_25.json", "w") as f:
            json.dump(data, f, indent=4)
            
        print("Saving to CSV...")
        with open("floorsheet_2026_03_25.csv", "w", newline='') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print("Done!")

if __name__ == "__main__":
    try:
        asyncio.run(fetch_and_save())
    except Exception as e:
        print(f"Error fetching historical floorsheet: {e}")
