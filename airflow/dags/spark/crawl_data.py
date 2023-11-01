from datetime import datetime, timedelta
import requests

def get_crash_data(days_ago=1):
    limit, offset = 10000, 0
    crashes_data = []
    current_time = datetime.now()
    filter_date = current_time - timedelta(days=days_ago)
    while True:
        response = requests.get(f"https://data.cityofchicago.org/resource/85ca-t3if.json?$where=crash_date>'{filter_date.date()}'&$limit={limit}&$offset={offset}")
        offset += 10000
        if len(response.json()) == 0:
            break
        if response.status_code != 200:
            break
        crashes_data.extend(response.json())
    return crashes_data

def get_people_data(days_ago=1):
    limit, offset = 10000, 0
    people_data = []
    current_time = datetime.now()
    filter_date = current_time - timedelta(days=days_ago)
    while True:
        response = requests.get(f"https://data.cityofchicago.org/resource/u6pd-qa9d.json?$where=crash_date>'{filter_date.date()}'&$limit={limit}&$offset={offset}")
        offset += 10000
        if len(response.json()) == 0:
            break
        if response.status_code != 200:
            break
        people_data.extend(response.json())
    return people_data

def get_vehicle_data(days_ago=1):
    limit, offset = 10000, 0
    vehicles_data = []
    current_time = datetime.now()
    filter_date = current_time - timedelta(days=days_ago)
    while True:
        response = requests.get(f"https://data.cityofchicago.org/resource/68nd-jvt3.json?$where=crash_date>'{filter_date.date()}'&$limit={limit}&$offset={offset}")
        offset += 10000
        if len(response.json()) == 0:
            break
        if response.status_code != 200:
            break
        vehicles_data.extend(response.json())
    return vehicles_data
