# здесь происходит парсинг данных с сайтов

import pandas as pd
import fastparquet as fpq
import sys
import requests
from bs4 import BeautifulSoup

URL = "https://top-rf.ru/places/110-rejting-regionov.html"
HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 OPR/92.0.0.0"
}


def get_html(url: str) -> str:
    rq = requests.get(url, headers=HEADERS)
    if rq.status_code != 200:
        sys.exit('Cannot parse the website!')
    else:
        return rq.text


def get_rating(website_text: BeautifulSoup) -> list:
    tables = website_text.find_all("table", {"style": "display: block; overflow-x: auto;"})
    ratings = list()
    for table in tables[1:]:
        for row in table.find_all("tr"):
            columns = row.find_all("td")
            if columns[2].find("p").get_text().replace(',', '') != "н/д":
                ratings.append(
                    (columns[1].find("p").get_text(), int(columns[2].find("p").get_text().replace(',', '')))
                )
    return ratings


# TODO:
# 0.) I don't need FederalDistricts.csv
# 1.) Divide Oktmo.csv into ~87 files.pqt by regions
# 2.) Use 2 files: Oktmo.csv and SettlementTypes.csv
# 3.) Make some settlements equal!
def create_pq_file(name: str, data: list) -> None:
    df = pd.DataFrame(data, columns=["region_name", "rating"])
    fpq.write(name, df)


def some_func():
    pass


if __name__ == "__main__":
    answer = input('''What do you want?\n
                    1 - to parse region ratings\n
                    2 - to parse settlement types''')
    if answer.strip('\n') == "1":
        html_text = get_html(URL)
        rating = get_rating(BeautifulSoup(html_text, 'lxml'))
        create_pq_file("region_rating.pqt", rating)
    elif answer.strip('\n') == "2":
        pass
    else:
        sys.exit("Did not understand your request!")
