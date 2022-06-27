# Software Development Assessment - Andrew Tomich

## Instructions 
* Write script to access API described at https://github.com/binance-us/binance-official-api-docs/blob/master/rest-api.md#http-return-codes
* The program should access the kline/candlestick data for BTCUSD and ETHUSD every 15 minutes and store the information in a csv.
* There should be a separate CSV for each coin.
* If the CSV does not already exist, create it and grab the last 30 days (minus today)
* If CSV does exist, get data from last date filled until yesterday.

