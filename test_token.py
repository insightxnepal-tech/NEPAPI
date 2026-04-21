from nepse.TokenUtils import TokenManager
from nepse import Nepse
import sys

sys.setrecursionlimit(5000)

def test_token():
    n = Nepse()
    print("Obtaining token...")
    try:
        token = n.token_manager.getAccessToken()
        print(f"Token: {token[:10]}...")
    except Exception as e:
        print(f"Token Error: {e}")

if __name__ == "__main__":
    test_token()
