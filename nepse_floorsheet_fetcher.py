import requests
import pandas as pd


def fetch_floor_sheet_data():
    url = 'https://nepalstock.com/api/floorsheet'  # Example endpoint, modify as needed
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()  # Assuming the response is in JSON format
    else:
        raise Exception('Failed to fetch data: ' + str(response.status_code))


def save_to_csv(data):
    df = pd.DataFrame(data)
    df.to_csv('floor_sheet_data.csv', index=False)


if __name__ == '__main__':
    try:
        data = fetch_floor_sheet_data()
        save_to_csv(data)
        print('Floor-sheet data fetched and saved as floor_sheet_data.csv')
    except Exception as e:
        print(e)