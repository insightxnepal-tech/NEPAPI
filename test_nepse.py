from nepse import Nepse
import sys

sys.setrecursionlimit(5000)

def test():
    n = Nepse()
    print("Fetching NBL...")
    try:
        data = n.getCompanyPriceVolumeHistory("NBL")
        print(f"Success! Found {len(data)} records.")
    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    test()
