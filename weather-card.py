#academic_palaniappan_seenivasan:Pj776P0siM@https://api.meteomatics.com/2024-06-03T00:00:00ZP1D:PT1H/t_2m:C,relative_humidity_2m:p/47.4245,9.3767/html?model=mix

import requests

# def get_weather(city, api_key):
#     # base_url = "http://api.openweathermap.org/data/2.5/weather"
#     base_url= "academic_palaniappan_seenivasan:Pj776P0siM@https://api.meteomatics.com/2024-06-03T00:00:00ZP1D:PT1H/t_2m:C,relative_humidity_2m:p/47.4245,9.3767/html?model=mix"
#     params = {
#         'q': city,
#         'appid': api_key,
#         'units': 'metric'  # Use 'imperial' for Fahrenheit
#     }    
    
#     response = requests.get(base_url, params=params)

#     if response.status_code == 200:
#         data = response.json()
#         return process_weather_data(data)
#     else:
#         return f"Error: {response.status_code}"


base_url= "academic_palaniappan_seenivasan:Pj776P0siM@https://api.meteomatics.com/2024-06-03T00:00:00ZP1D:PT1H/t_2m:C,relative_humidity_2m:p/47.4245,9.3767/html?model=mix"
print(requests.get(base_url,''))