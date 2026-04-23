import requests
url = "https://opensky-network.org/api/states/all"


def fetch_data():
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occured: {e}")
        raise


fetch_data()
