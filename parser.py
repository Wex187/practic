import requests
from urllib.parse import quote
from bs4 import BeautifulSoup
import psycopg2

# Настройки подключения к базе данных
connection = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="12365477Suka",
    database="vac",
)
connection.autocommit = True
cursor = connection.cursor()

# Функция для парсинга вакансий
def parse_vacancies(job_title, city, salary_min):
    # Явно указываем ID региона для Москвы
    area_id = 1  # ID региона для Москвы в API HeadHunter

    url = f'https://hh.ru/search/vacancy?text={quote(job_title)}&area={area_id}&salary={salary_min}'
    print(url)  # Добавлено для отладки
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Проверяем статус ответа

        soup = BeautifulSoup(response.text, 'html.parser')
        vacancies = []

        for vacancy in soup.find_all('div', class_='vacancy-serp-item'):
            title = vacancy.find('a', class_='bloko-link').text.strip()
            company = vacancy.find('a', class_='bloko-link_secondary').text.strip()
            experience = vacancy.find('div', class_='vacancy-serp-item__sidebar').text.strip()
            location = vacancy.find('span', {'data-qa': 'vacancy-serp__vacancy-address'}).text.strip()
            salary = vacancy.find('span', {'data-qa': 'vacancy-serp__vacancy-compensation'})
            salary_text = salary.text.strip() if salary else 'Не указана'
            url = vacancy.find('a', class_='bloko-link')['href']

            metro = vacancy.find('span', class_='vacancy-serp-item__meta-info')
            metro_text = metro.text.strip() if metro else 'Не указано'

            vacancies.append({
                'title': title,
                'company': company,
                'experience': experience,
                'city': location,
                'metro': metro_text,
                'salary': salary_text,
                'url': url
            })

        return vacancies

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return []

# Функция для добавления вакансий в базу данных с проверкой на дубликаты
def add_vacancies_to_db(vacancies):
    try:
        for vacancy in vacancies:
            cursor.execute(f"SELECT COUNT(*) FROM vacancies WHERE url = '{vacancy['url']}'")
            if cursor.fetchone()[0] == 0:
                cursor.execute(
                    f"INSERT INTO vacancies (title, company, experience, city, metro, salary, url) "
                    f"VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (vacancy['title'], vacancy['company'], vacancy['experience'], vacancy['city'], vacancy['metro'], vacancy['salary'], vacancy['url'])
                )
        connection.commit()  # Применение изменений
        print("Данные успешно добавлены в базу данных.")
    except psycopg2.Error as e:
        print(f"Ошибка при добавлении данных в базу данных: {e}")

# Пример использования функции
vacancies = parse_vacancies('Python разработчик', 'Москва', '100000')
print(vacancies)  # Проверка, что вакансии успешно спарсились
add_vacancies_to_db(vacancies)


