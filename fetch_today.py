import asyncio
import csv
import sys
from nepse import AsyncNepse
import tqdm.asyncio
from datetime import date

FETCH_ATTEMPTS = 3

async def fetch_floorsheet():
    print("Initializing AsyncNepse...")
    nepseAsync = AsyncNepse()
    nepseAsync.setTLSVerification(False)

    print("Fetching today's floorsheet...")
    # getFloorSheet() fetches the latest session's data
    for attempt in range(1, FETCH_ATTEMPTS + 1):
        try:
            final_data = await nepseAsync.getFloorSheet(show_progress=True)
            break
        except Exception as e:
            # One stalled page loses the whole session, and NEPSE only ever serves
            # the latest one, so a dropped session can never be fetched again.
            print(f"Attempt {attempt}/{FETCH_ATTEMPTS} failed: {type(e).__name__}: {e}")
            if attempt == FETCH_ATTEMPTS:
                raise

    if not final_data:
        print("No floorsheet data found.")
        return []

    print(f"Got {len(final_data)} records.")
    return final_data

def session_date(data):
    """Date of the session the rows belong to.

    NEPSE keeps serving the previous session until a new one closes, so naming
    files after the local date stamps weekend and pre-market fetches with a date
    the data is not from.
    """
    dates = {str(row.get("businessDate") or "")[:10] for row in data}
    dates.discard("")
    if not dates:
        print("No businessDate in the response, falling back to today's date.")
        return date.today().strftime("%Y-%m-%d")
    return max(dates)

async def main():
    data = await fetch_floorsheet()

    if data:
        csv_filename = f"floorsheet_{session_date(data)}.csv"
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
        print(f"Error: {type(e).__name__}: {e}")
        sys.exit(1)
