# scripts/test_auth.py

from centrepoint.auth import CentrePointAuth

def main():
    auth = CentrePointAuth()
    token = auth.get_token()
    print("\n✅ Access Token retrieved successfully!")
    print(f"Token (first 60 chars): {token[:60]}...\n")

if __name__ == "__main__":
    main()

