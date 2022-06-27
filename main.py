#!.venv/bin/python
import time
import requests
import os
import csv
from datetime import datetime

BASE_URL =  'https://api.binance.us'
CANDLESTICK_ENDPOINT = '/api/v3/klines'

class DataHandler():
    def __init__(self, symbol): # INSTANTIATE DATAHANDLER CLASS AS EITHER BTC OR ETH HANDLER

        self.data_header = {
            'accept': 'application/xhtml+xml'
        }

        self.symbol = symbol 

        self.url = BASE_URL + CANDLESTICK_ENDPOINT 

        self.filename= self.symbol + '.csv'


    def create_csv(self): # CREATE CSV FILE, WITH HEADERS, IF IT DOES NOT ALREADY EXIST
        with open(self.filename, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['Open Time',
            'Open',
            'High',
            'Low',
            'Close',
            'Volume',
            'Close Time',
            'Quote Asset Volume',
            'Number of Trades',
            'Taker Buy Base Asset Volume',
            'Taker Buy Quote Asset Volume',
            'Ignore'])



    def check_csv(self): # CHECK FOR EXISTENCE OF CSV FILE IN QUESTION
        if os.path.isfile(self.filename):
            return True
        self.create_csv()
        return False



    def write_new_data(self, data): # WRITE NEW DATA TO CSV FILE ONCE IT EXISTS 
        with open(self.filename, 'a', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerows(data)



    def get_last_data(self): # IF CSV FILE ALREADY EXISTS (ESPECIALLY AFTER 419 HTTP RESPONSE) READ DATE OF LAST CLOSE    
        with open(self.filename, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            contents = None
            for row in csvreader:
                contents = row[6] # GET MOST RECENT CLOSE DATE
            return datetime.fromtimestamp(int(contents) / 1000)



    def get_data(self): # RETRIEVE NEW DATA FROM API ENDPOINT

        qString = f"symbol={self.symbol}USD&interval=1d&limit=" # QUERY STRING ASSIGNED WITHOUT LIMIT

        days_to_pull = 30 # DEFAULT NUMBER OF DAYS, USED IF CSV DOES NOT EXIST

        if self.check_csv(): # IF CSV EXISTS, CHECK FOR INTERVAL BETWEEN LAST DATE PULLED AND TODAY
            days_from_last_data = datetime.today() - self.get_last_data()

            days_to_pull = days_from_last_data.days - 1

        if 0 < days_to_pull: # PULL LIMIT IS 1000
            qString += str(days_to_pull) # APPEND LIMIT TO QUERY STRING

            r = requests.get(url=self.url, params=qString, headers=self.data_header)
            r.raise_for_status()
            if 200 <= r.status_code <= 229:

                data= r.json()
                self.write_new_data(data)

                return True, f"Pulled {days_to_pull} Days." # SUCCESSFUL RETRIEVAL, WITH ACCESS TO NUMBER OF DAYS PULLED

            if r.status_code == 418 or r.status_code == 429: # IF RATE LIMIT IS REACHED, RETRIEVE THE TIME TO WAIT
                return False, r.headers['Retry-After']


        return True, 'No Days to Pull!'

# MAIN FUNCTION
def main():

    btcHandler = DataHandler('BTC')
    ethHandler = DataHandler('ETH')

    while True:

        btc_wait_time = eth_wait_time = 0

        if not (btc := btcHandler.get_data())[0]:
            btc_wait_time = int(btc[1]) # IF RATE LIMIT IS REACHED, STORE ASSIGNED WAIT TIME
        if not (eth := ethHandler.get_data())[0]:
            eth_wait_time = int(eth[1]) # IF RATE LIMIT IS REACHED, STORE ASSIGNED WAIT TIME

        time.sleep(max((60 * 15), btc_wait_time, eth_wait_time)) # WAIT FOR MAX VALUE BETWEEN 15 MINUTES OR ASSIGNED WAIT TIME FROM RATE LIMIT


if __name__ == '__main__':
    main()
