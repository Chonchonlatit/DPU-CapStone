import json
import requests

    # API_KEY = os.environ.get("WEATHER_API_KEY")
    # API_KEY = Variable.get("weather_api_key")
if __name__ == "__main__":
    payload = {
        "city": "Bangkok",
        "state": "Bangkok",
        "country": "Thailand",
        "key": "2eaaa008-13eb-45d5-ae38-488fe66ecfeb"
    }
    url = "https://api.airvisual.com/v2/city"
    response = requests.get(url, params=payload)
    print(response.url)

    data = response.json()
    print(data)
    
    with open("AQI.json", "w") as f:
        json.dump(data, f)

