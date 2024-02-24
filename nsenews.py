import requests
import time

#declaring the URL
url = "https://www.nseindia.com/api/corporate-announcements?index=equities"

#Declaring headers one and two for two separate sessions
header_one = {
    'authority': 'www.nseindia.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ms;q=0.8,ar;q=0.7',
    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
}

header_two = {
    'authority': 'www.nseindia.com',
    'method' : 'GET',
    'path' : '/api/corporate-announcements?index=equities',
    'scheme' : 'https',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ms;q=0.8,ar;q=0.7',
    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="122"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    "user-agent":"--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
}


#creating session one
session = requests.Session()

first = session.get(url, headers = header_one)

#function for session one
def first_session():
    if first.status_code == 200:
     print(first.status_code)
     print(first.text)
    elif first.status_code == 401:
     print("The first session got failed")
     return("FAIL")

first_session()

#sleep function to emulate browser
time.sleep(3)

#creating session two which will run with the second header
session_two = requests.Session()
second = session_two.get(url, headers = header_two)

def second_session():
 if second.status_code == 200:
    print(second.status_code)
    print(second.text)
    print("This is by second session")
 elif second.status_code == 401:
    print("The second session got failed")
    return("FAIL")
    
second_session()
