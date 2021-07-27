from bs4 import BeautifulSoup
import requests
import pandas as pd
import boto3

dynamodb = boto3.resource('dynamodb')
openinsider = dynamodb.Table('openinsider')
FILING_DATE = 'Filing Date'
LAST_FILING_DATE = 'last_filing_date'

def lambda_handler(event, context):
    url = 'http://openinsider.com/latest-cluster-buys'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class' : 'tinytable'})
    df = build_dateframe_from_soup_table(table)

    df2 = get_data_later_than(df, get_last_checked_date())
    
    list_of_new_tickers = df2['Ticker'].to_list()

    res = ''
    if (list_of_new_tickers):
       send_to_sns(list_of_new_tickers)
       res = save_new_date(df2.head(1)[FILING_DATE])
    
    return {
        'statusCode': 200,
        'body': res 
    }

def get_data_later_than(dataframe, date_string):
    return dataframe[dataframe[FILING_DATE] > date_string].copy()

def get_last_checked_date_test():
    return '2021-07-23 17:06:15'

def get_last_checked_date():
    response = openinsider.get_item(
        Key={
            'id' : 1
        }
    )
    return response['Item'][LAST_FILING_DATE]

def save_new_date(date_string):
    openinsider.put_item(
        Item={
            'id' : 1,
            LAST_FILING_DATE : date_string
        }
    )

def send_to_sns(tickers):
    message = ", ".join(tickers)
    return message

def build_dateframe_from_soup_table(table):
    table_head = table.find('thead')
    table_body = table.find('tbody')
    tr = table_head.find('tr')
    th = tr.find_all('th')

    headers = []

    for r in th:
        headers.append(r.h3.string)

    data_rows = table_body.find_all('tr')

    data = []
    for r in data_rows:
        cols = r.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append(cols)

    return pd.DataFrame(data, columns=headers) 

if __name__ == '__main__':
    lambda_handler(None, None)