import asyncio
import csv
import sys
from nepse import AsyncNepse
import tqdm.asyncio
from datetime import date

FETCH_ATTEMPTS = 3

class IncompleteFloorsheet(RuntimeError):
    pass

async def reported_total(nepseAsync):
    """Row count NEPSE says the latest session has, or None if it won't say.

    getFloorSheet() turns a failed page into an empty list, so a short fetch
    looks exactly like a light trading day unless it is checked against this.
    """
    try:
        url = (
            f"{nepseAsync.api_end_points['floor_sheet']}"
            f"?&size={nepseAsync.floor_sheet_size}&sort=contractId,desc"
        )
        sheet = await nepseAsync.requestPOSTAPI(
            url=url, payload_generator=nepseAsync.getPOSTPayloadIDForFloorSheet
        )
    except Exception as e:
        # Skipping the check beats abandoning a session that cannot be re-fetched.
        print(f"Could not read the expected row count: {type(e).__name__}: {e}")
        return None
    if not sheet or "floorsheets" not in sheet:
        return None
    return sheet["floorsheets"].get("totalElements")

async def fetch_floorsheet():
    print("Initializing AsyncNepse...")
    nepseAsync = AsyncNepse()
    nepseAsync.setTLSVerification(False)

    print("Fetching today's floorsheet...")
    # getFloorSheet() fetches the latest session's data
    for attempt in range(1, FETCH_ATTEMPTS + 1):
        try:
            # Read the expected count first: during a session it can only grow,
            # so a later count never turns a complete fetch into a failure.
            expected = await reported_total(nepseAsync)
            final_data = await nepseAsync.getFloorSheet(show_progress=True)
            got = len({row.get("contractId") for row in final_data})
            if expected and got < expected:
                raise IncompleteFloorsheet(
                    f"got {got} of {expected} contracts, {expected - got} missing"
                )
            break
        except Exception as e:
            # A dropped or stalled page loses part of the session, and NEPSE only
            # ever serves the latest one, so it can never be fetched again.
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
