import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
import json
import time

# Kafka producer setup with JSON serializer
producer = KafkaProducer(
    bootstrap_servers='localhost:8000',

)
print("connected", producer)
producer.send('amazon', "SENT1")
producer.flush()
# # Function to scrape iPhone products from Amazon
# def scrape_amazon_iphones():
#     url = "https://www.amazon.in/s?k=iphone"
#     HEADERS = {
#         'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
#         'Accept-Language': 'en-US,en;q=0.5'
#     }
#     response = requests.get(url, headers=HEADERS)
#     soup = BeautifulSoup(response.content, "html.parser")
#     products = soup.find_all("div", {"data-component-type": "s-search-result"})

#     for product in products:
#         # Extract title safely
#         title_tag = product.find('h2')
#         title = title_tag.text.strip() if title_tag else None

#         # Extract product URL safely
#         if title_tag and title_tag.a and 'href' in title_tag.a.attrs:
#             product_url = "https://www.amazon.in" + title_tag.a['href']
#         else:
#             print("yes")
#             product_url = "None"

#         # Extract price parts safely
#         price_whole = product.find("span", class_="a-price-whole")
#         price_frac = product.find("span", class_="a-price-fraction")
#         price = None
#         if price_whole and price_frac:
#             price = f"{price_whole.text}{price_frac.text}"
#         elif price_whole:
#             price = price_whole.text.strip()

#         if title and product_url:
#             product_data = {
#                 'title': title,
#                 'url': product_url,
#                 'price': price or "NA"
#             }
#             # Send product data to Kafka topic 'amazon'
#             producer.send('amazon', product_data)
#             print(f"Sent to Kafka: {product_data}")
#             time.sleep(0.1)  # slight delay to avoid flooding

#     producer.flush()


if __name__ == "__main__":
    scrape_amazon_iphones()
