import requests
from bs4 import BeautifulSoup
import pandas as pd

from utils import clean_text, parse_date


response = requests.get('https://www.tbank.ru/invest/social/hashtag/нефть')
response.encoding = 'utf-8'

soup = BeautifulSoup(response.text, 'html.parser')
posts = soup.find_all('div', class_=re.compile(r'social-hashtagpage__gXTx8X'))

data = []
for post in posts:
    username = post.find('span', class_=re.compile(r'social-hashtagpage__aSULlZ'))
    username = username.get_text(strip=True) if username else "N/A"

    date = post.find('div', class_=re.compile(r'social-hashtagpage__cSULlZ'))
    date = parse_date(date.get_text(strip=True)) if date else "N/A"

    text = post.find('div', class_=re.compile(r'social-hashtagpage__ffTK6Z'))
    text = clean_text(text.get_text(strip=True)) if text else "N/A"

    likes = post.find('div', class_=re.compile(r'social-hashtagpage__ei8mu9'))
    likes = likes.get_text(strip=True) if likes else "0"

    data.append([date, text, likes, username])


df = pd.DataFrame(data, columns=['Date', 'Text', 'Likes', 'Author']).set_index('Date')
df.to_csv('pulse.csv')

print("Данные сохранены в posts.csv")

