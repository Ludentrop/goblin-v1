import re
from datetime import datetime, timedelta


def clean_text(text):
    text = re.sub(r'[^\w\s.,!?а-яА-ЯёЁ]', ' ', text, flags=re.UNICODE)
    text = re.sub(r'\$\w+\b', '', text)
    text = re.sub(r'#\w+\b', '', text)
    return ' '.join(text.split()).strip()


def parse_date(date_str):
    months = {
        'января': '01', 'февраля': '02', 'марта': '03',
        'апреля': '04', 'мая': '05', 'июня': '06',
        'июля': '07', 'августа': '08', 'сентября': '09',
        'октября': '10', 'ноября': '11', 'декабря': '12'
    }

    today = datetime.now()

    if "Сегодня" in date_str:
        return today.strftime("%d.%m.%Y")
    elif "Вчера" in date_str:
        return (today - timedelta(days=1)).strftime("%d.%m.%Y")
    else:
        for ru_month, num_month in months.items():
            if ru_month in date_str:
                date_str = date_str.replace(ru_month, num_month)
                break
        try:
            date_obj = datetime.strptime(date_str, "%d %m %Y")
            return date_obj.strftime("%d.%m.%Y")
        except:
            return date_str
