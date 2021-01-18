import requests
import json
import sys
import configparser
import datetime
from datetime import date
from influxdb import InfluxDBClient
import argparse

config = configparser.ConfigParser()
config.read("config.ini")

parser=argparse.ArgumentParser(
    description='''Script to login to E-REDES distribución of the spanish energy company and download consumptions and save to a file and influxdb. ''')
args=parser.parse_args()

def login():
    user = config.get("USER", "username")
    password = config.get("USER", "password")
    url = config.get("URL", "url_login")
    apikey = config.get("URL", "apikey")
  
    headers = {
        "Content-Type": "application/json",
        "apikey": apikey
    }

    payload = {
        "jsonrpc": "2.0",
        "method": "login",
        "id": 1607631285241,
            "params": {
                "document": user,
                "password": password
            }
    }
    print('Login to e-redes...')

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    print(response.status_code)

    print('Login success!')

    return json.loads(response.json()["result"])


def get_consumption(start_date, end_date, cups, access_token):
    url = config.get("URL", "url_consumption")
    apikey = config.get("URL", "apikey")
  
    headers = {
        "Content-Type": "application/json",
        "apikey": apikey,
        "sessionkey": access_token
    }

    payload = {
        "jsonrpc": "2.0",
        "method": "getConsumos",
        "id": 1607631242463,
            "params": {
                    "cups": cups,
                    "fechaInicio": start_date,
                    "fechaFin": end_date
            }
    }
    print('Retrieving consumption data')

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    print(response.status_code)
    print('Retrieving data success!')

    return response.json()


def save_to_influx(result_json):
    influxdb_host = config.get("INFLUXDB", "host")
    influxdb_port = config.get("INFLUXDB", "port")
    influxdb_db = config.get("INFLUXDB", "db")

    influxdb_client = InfluxDBClient(host=influxdb_host, port=influxdb_port, database=influxdb_db)

    measurements = []

    for hour in result_json:
        date = hour['datetime']

        if "24:00" in date:
            date = datetime.datetime.strptime(date.replace("24:00","00:00"), '%d/%m/%Y %H:%M') + datetime.timedelta(days=1)
        else:
            date = datetime.datetime.strptime(date, '%d/%m/%Y %H:%M')

        date = date - datetime.timedelta(hours=1)

        row = dict()
        row['measurement'] = "electricity_consumption"
        row['time'] = date
        row['fields'] = { "kWh": hour['consumo'] }
        measurements.append(row)

    influxdb_client.write_points(measurements)


def save_to_file(start_date, end_date, result_json):
    filename = 'consumo-' + start_date.replace("/","_") + "-" + end_date.replace("/","_") + ".json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(result_json, f)


def last_day_of_month(any_day):
    # this will never fail
    # get close to the end of the month for any day, and add 4 days 'over'
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # subtract the number of remaining 'overage' days to get last day of current month, 
    # or said programatically said, the previous day of the first of next month
    return next_month - datetime.timedelta(days=next_month.day)

def main():
    start_date = (date.today() - datetime.timedelta(days=30)).replace(day=1).strftime('%d/%m/%Y')
    end_date = last_day_of_month((date.today() - datetime.timedelta(days=30)).replace(day=1)).strftime('%d/%m/%Y')
    
    print(start_date)
    print(end_date)

    login_info = login()
    access_token = login_info["accessToken"]
    cups = login_info["cups"]

    print('cups: ' + cups)
    print('access_token: ' + access_token)

    consumption = get_consumption(start_date, end_date, cups, access_token)
    result_json = json.loads(json.dumps(json.loads(consumption["result"]), indent = 3))

    save_to_influx(result_json)
    save_to_file(start_date, end_date, result_json)


if __name__ == "__main__":
    main()
