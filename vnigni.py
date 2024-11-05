import requests
import pandas as pd
import time
import re
import logging
from requests.exceptions import RequestException, ConnectionError, HTTPError, Timeout
import zipfile
from http.client import IncompleteRead
import tempfile
import os
from collections import Counter


def main():
    logging.basicConfig(filename='data.log', level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    def get_guid(base_url):
        filters = {
            "filters": {
                "attr": [],
                "fulltext": None
            }
        }
        MAX_RETRIES = 5
        RETRIES = 0
        INDEX = 0
        SLEEP_TIME = 35
        guid_list = []

        while RETRIES < MAX_RETRIES:
            try:
                url = f"{base_url}{INDEX}"
                response = requests.post(url, json=filters)

                # Проверка статуса ответа
                if response.status_code == 200:
                    data = response.json()
                    if objects := data.get("objects"):
                        for obj in objects:
                            guid_list.append(obj["guid"])
                    else:
                        logging.info(f"Response with NULL 'objects':  {url}")
                        break
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 10))
                    logging.warning(f"429 {retry_after} sec {url}")
                    time.sleep(retry_after)
                else:
                    logging.error(f"Error {url}: {response.status_code} - {response.reason}. Response: {response.text}")
                    break
            except requests.exceptions.Timeout:
                RETRIES += 1
                logging.error(f"Error {url}: Retry {RETRIES} / {MAX_RETRIES}.")
                time.sleep(SLEEP_TIME)
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to retrieve data after {MAX_RETRIES} retries: {e}")
                time.sleep(SLEEP_TIME)
                break
            INDEX += 1

        # Создание DataFrame из списка guid
        df_guid = pd.DataFrame(guid_list, columns=["guid"])
        return df_guid

    def make_request_with_retry(url, headers=None):
        MAX_RETRIES =3
        RETRIES = 0
        SLEEP_TIME = 40
        while RETRIES < MAX_RETRIES:
            try:
                response = requests.get(url, headers=headers, stream=True)
                logging.debug(f"Request {url}: Status {response.status_code}")
                if response.status_code == 200:
                    logging.info(f"Request {url} succeed. Code 200.")
                    return response
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logging.warning(f"429 {retry_after} sec.")
                    time.sleep(retry_after)
                    RETRIES += 1
                else:
                    logging.error(
                        f"Error {url}: {response.status_code} - {response.reason}. Response: {response.text}")
                    break
            except (RequestException, IncompleteRead) as e:
                RETRIES += 1
                logging.error(f"Error {url}: {e}. Retry {RETRIES} / {MAX_RETRIES}.")
                if RETRIES < MAX_RETRIES:
                    time.sleep(SLEEP_TIME)
                else:
                    logging.error(f"Failed to retrieve data after {MAX_RETRIES} retries: {e}")
                    raise RuntimeError(f"Failed to retrieve data after {MAX_RETRIES} retries: {e}")
        return None

    def fetch_data_with_retry(session, url, headers=None):
        SLEEP_TIME = 40
        RETRIES = 0
        MAX_RETRIES = 5
        while RETRIES < MAX_RETRIES:
            try:
                response = session.get(url, headers=headers)
                logging.debug(f"Request {url}: Status {response.status_code}")
                if response.status_code == 200:
                    logging.info(f"Request {url} succeed. Code 200.")
                    return response.json()
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 10))
                    logging.warning(f"429 {retry_after} sec.")
                    time.sleep(retry_after)
                else:
                    logging.error(
                        f"Error {url}: {response.status_code} - {response.reason}. Response: {response.text}")
                    return None
            except (RequestException, ConnectionError, HTTPError, Timeout) as e:
                RETRIES += 1
                logging.error(f"Error {url}: {e}. Retry {RETRIES} / {MAX_RETRIES}.")
                if RETRIES < MAX_RETRIES:
                    time.sleep(SLEEP_TIME)
                else:
                    logging.error(f"Failed to retrieve data after {MAX_RETRIES} retries: {e}")
                    raise RuntimeError(f"Failed to retrieve data after {MAX_RETRIES} retries: {e}")

    def parse_all(base_url, base_url_table, df):
        # Определение столбцов
        COLUMNS = ['Площадь', '№ скважины', 'URL', 'coordinates', 'Административно-территориальная привязка',
                   'Нефтегазогеологическое районирование', 'Тектоническая позиция (краткое обобщенное наименование)',
                   'Начало бурения', 'Окончание бурения',
                   'Наименование организации, проводившей бурение', 'Фактическая глубина, м',
                   'Фактический горизонт (стратиграфический интервал вскрытия)', 'Проектный горизонт',
                   'Состояние скважины', 'Тип скважины', 'Профиль скважины', 'Причина ликвидации',
                   'Задачи, возложенные на скважину', 'Геологические результаты', 'Испытания',
                   'Параметры бурового раствора',
                   'Конструкция скважины проектная', 'Конструкция скважины фактическая', 'Интервалы',
                   'Стратиграфические разрезы'
                   ]
        # Создание DataFrame с указанными столбцами
        data_pd = pd.DataFrame(columns=COLUMNS)
        interval_details = []
        rows = []

        for guid in df['guid']:
            well_url = base_url + guid
            table_url = base_url_table + guid
            data = fetch_data_with_retry(session, well_url, headers=headers)
            if data is not None:
                new_row = {}
                # Проверка наличия ключа "geometry"
                if 'geometry' in data and data['geometry'].get('has') == "true":
                    geometry_data = data['geometry']
                    lat = geometry_data.get('lat')
                    lon = geometry_data.get('lon')
                    coordinates_wkt = f"POINT ({lat} {lon})"
                    new_row.update({'URL': table_url})
                    new_row.update({'coordinates': coordinates_wkt})

                    for attr in data.get('attributes', []):
                        caption = attr.get('caption')
                        value = attr.get('value')
                        if caption in ["Начало бурения", "Окончание бурения"] and isinstance(value, dict):
                            day = value.get('d')
                            month = value.get('m')
                            year = value.get('y')
                            if year is not None:
                                date_parts = []
                                if day is not None:
                                    date_parts.append(f"{int(day):02d}")
                                if month is not None:
                                    date_parts.append(f"{int(month):02d}")
                                date_parts.append(year)

                                date_value = "-".join(date_parts)
                                new_row.update({caption: date_value})
                        else:
                            value_to_insert = value.get('title') if isinstance(value, dict) else value
                            if caption in COLUMNS:
                                new_row.update({caption: value_to_insert})

                        if caption == "Интервалы" and isinstance(value, list):
                            for interval in value:
                                interval_guid = interval.get('guid')
                                if interval_guid:
                                    interval_data = fetch_interval_data(interval_guid)
                                    if interval_data:
                                        interval_details.append(interval_data)

                        if interval_details:
                            new_row.update({'Интервалы': "\n".join(interval_details)})
                            interval_details = []

                        if caption == "Стратиграфические разрезы" and isinstance(value, list):
                            section_guid = value[0].get('guid')
                            if section_guid:
                                section_data = fetch_section_data(section_guid)
                                if section_data:
                                    new_row.update({caption: section_data})
                        if caption == "Наличие файлов" and value:
                            title = data.get("geometry", {}).get("title", "Название не найдено")
                            download_files_from_json(guid, title, "D:\Vnigni")
                    rows.append(new_row)
            else:
                continue

        data_pd = pd.DataFrame(rows, columns=COLUMNS)
        data_pd['Название'] = data_pd['№ скважины'].astype(str) + ' - ' + data_pd['Площадь'].astype(str)
        data_pd.drop(['№ скважины', 'Площадь'], axis=1, inplace=True)

        # Вставка столбца "Название" на первую позицию
        data_pd.insert(0, 'Название', data_pd.pop('Название'))
        return data_pd

    def fetch_interval_data(interval_guid):
        interval_url = f"https://kern.vnigni.ru/api/kern/object/{interval_guid}"
        interval_data = fetch_data_with_retry(session, interval_url, headers=headers)
        details = []
        if interval_data is not None:
            if 'attributes' in interval_data:
                for attr in interval_data['attributes']:
                    caption = attr.get('caption')
                    value = attr.get('value', {}).get('title') if isinstance(attr.get('value'), dict) else attr.get(
                        'value')
                    if caption in ["Номер по скважине (сверху вниз)", "Интервал отбора керна, м - от",
                                   "Интервал отбора керна, м - до",
                                   "Вынос керна, м - информация из геологической документации по скважине",
                                   "Диаметр керна, мм", "Стратиграфический возраст",
                                   "Наименование породы (по геологической документации)"]:
                        details.append(f"{caption}: {value}")

                # Объединение всех деталей в одну строку
            return ", ".join(details)
        else:
            return None

    def fetch_section_data(section_guid):
        section_url = f"https://kern.vnigni.ru/api/kern/object/{section_guid}"
        section_data = fetch_data_with_retry(session, section_url, headers=headers)
        details_string = ""

        if section_data is not None:
            if 'attributes' in section_data:
                for attr in section_data['attributes']:
                    caption = attr.get('caption')
                    if caption == "Стратоны":
                        value = attr.get('value')
                        if isinstance(value, list):
                            details = []
                            for item in value:
                                title = item.get('title', '')
                                note = item.get('note', '')
                                detail_string = f"{title} ({note})" if note else title
                                details.append(detail_string)
                            details_string = ", ".join(details)

        return details_string if details_string else None

    def download_files_from_json(url, archive_name, dir):
        archive_name += '.zip'
        if not os.path.exists(dir):
            os.makedirs(dir)
        archive_path = os.path.join(dir, archive_name)
        with tempfile.TemporaryDirectory() as temp_dir:
            file_paths = []
            json_url = f"https://kern.vnigni.ru/api/kern/object/documents/{url}"
            json_data = make_request_with_retry(json_url).json()

            filename_counts = Counter()

            for item in json_data:
                for file_info in item.get('value', []):
                    file_guid = file_info.get('guid')
                    file_name = file_info.get('filename')
                    if file_guid and file_name:
                        file_url = f"https://kern.vnigni.ru/api/kern/object/document/{file_guid}"

                        filename_counts[file_name] += 1
                        if filename_counts[file_name] > 1:
                            file_name = f"{os.path.splitext(file_name)[0]}_{filename_counts[file_name]}{os.path.splitext(file_name)[1]}"

                        filename = os.path.join(temp_dir, file_name)

                        for attempt in range(3):
                            try:
                                response = make_request_with_retry(file_url)
                                if response:
                                    with open(filename, 'wb') as f:
                                        f.write(response.content)
                                    file_paths.append(filename)
                                    logging.info(f"File {file_name} has been successfully downloaded.")
                                    break
                                else:
                                    logging.error(f"Failed to load file {file_url}.")
                            except IncompleteRead as e:
                                logging.error(f"Error loading {file_url}: {e}")
                                if attempt < 2:
                                    logging.info("Re-attempting to download...")
                                    time.sleep(5)
                            except Exception as e:
                                logging.error(f"Unknown error loading {file_url}: {e}")
                            break

            # Создание архива и добавление файлов в него
            try:
                with zipfile.ZipFile(archive_path, 'w') as zipf:
                    for file_path in file_paths:
                        zipf.write(file_path, os.path.basename(file_path))
                logging.info(f"The {archive_path} archive has been successfully created.")
            except Exception as e:
                logging.error(f"Error creating archive {archive_path}: {e}")

    def extract_report(row):
        prefix = '; '
        if isinstance(row, str) and prefix in row:
            match = re.search(r'; (отчет)(.*?)(\n|$)', row.lower())
            if match:
                return match.group(1).strip() + match.group(2).strip()
        return None

    def remove_report(row):
        prefix = '; '
        if isinstance(row, str) and prefix in row:
            parts = row.split(prefix, 1)
            return parts[0].strip()
        return row

    def report_file(df):
        if 'Отчеты' not in df.columns:
            df.insert(11, 'Отчеты',
                      df['Фактический горизонт (стратиграфический интервал вскрытия)'].apply(extract_report))
            df['Фактический горизонт (стратиграфический интервал вскрытия)'] = df[
                'Фактический горизонт (стратиграфический интервал вскрытия)'].apply(remove_report)
        return df

    def check_oil_gas(testing_text):
        if isinstance(testing_text, str):
            lower_text = testing_text.lower()
            contains_oil = 'нефт' in lower_text or 'qн' in lower_text
            contains_gas = 'газ' in lower_text or 'qг' in lower_text

            if contains_oil and contains_gas:
                return 'нефть и газ'
            elif contains_oil:
                return 'нефть'
            elif contains_gas:
                return 'газ'
        return None

    def testing_file(df):
        df.insert(20, 'Притоки', df['Испытания'].apply(check_oil_gas))
        return df

    def extract_report_number(text):
        if isinstance(text, str):
            match = re.search(r'отчет№ (\d+)', text)
            if match:
                return match.group(1)
        return None

    def number_file(df):
        df['Отчеты'] = df['Отчеты'].astype(str).apply(extract_report_number)
        return df

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    session = requests.Session()

    base_url_guid = "https://kern.vnigni.ru/api/kern/catalog/well/simple/"
    base_url_well = "https://kern.vnigni.ru/api/kern/object/"
    base_url_table = "https://kern.vnigni.ru/well/catalog/"

    df_guid = get_guid(base_url_guid)
    up_df = parse_all(base_url_well, base_url_table, df_guid)
    up_df = number_file(testing_file(report_file(up_df)))

    up_df.to_csv("csv/vnigni_26_07.csv", index=False)


if __name__ == "__main__":
    main()
