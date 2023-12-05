import re
import requests
from bs4 import BeautifulSoup
import pandas as pd


def fetch_imdb_data():
    movie_data = []
    movies_per_page = 50
    total_movies = 0
    total_pages = 0

    url = 'https://www.imdb.com/search/title/?country_of_origin=ID'
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'html.parser')

    # Extracting the total number of movies
    count_text = soup.find('div', class_='desc').span.text
    total_movies = int(count_text.split()[2].replace(',', ''))
    total_pages = (total_movies // movies_per_page) + 1

    for i in range(1, total_pages + 1):
        print(f"Fetching data of {i} of {total_pages}...")
        start_index = (i - 1) * movies_per_page
        url = f'https://www.imdb.com/search/title/?country_of_origin=ID&start={start_index}'
        res = requests.get(url)
        soup = BeautifulSoup(res.content, 'html.parser')
        movie_list = soup.find_all('div', class_='lister-item mode-advanced')

        for movie in movie_list:
            # Initialize movie details as empty strings
            title = ''
            year = ''
            genre = ''
            rating = ''
            duration = ''
            description = ''
            stars = []
            directors = []
            if title_element := movie.h3.a:
                title = title_element.text.strip()

            if year_element := movie.find('span', class_='lister-item-year'):
                year = year_element.text.strip('()')

            if genre_element := movie.find('span', class_='genre'):
                genre = genre_element.text.strip()

            if rating_element := movie.find('strong'):
                rating = rating_element.text

            if duration_element := movie.find('span', class_='runtime'):
                duration = duration_element.text.strip(' min')

            if description_element := movie.find_all('p', class_='text-muted')[
                -1
            ]:
                description = description_element.text.strip()

            if stars_directors_element := movie.find('p', class_='').find_all(
                    'a', href=True
            ):
                star_director_list = stars_directors_element[0].parent.contents
                data_cleaned = [re.sub('<.*?>', '', str(element)).strip()
                                for element in star_director_list if str(element).strip()]
                data_cleaned = [element for element in data_cleaned if element not in ['', ',', '|']]

                director_index = next(
                    (i for i, element in enumerate(data_cleaned) if element in ['Director:', 'Directors:']), None
                )

                stars_index = next(
                    (i for i, element in enumerate(data_cleaned) if element in ['Stars:', 'Star:']), None
                )

                directors = data_cleaned[
                            director_index + 1:stars_index] if director_index is not None and stars_index is not None else []
                stars = data_cleaned[stars_index + 1:] if stars_index is not None else []

            movie_data.append({'Title': title, 'Year': year, 'Genre': genre, 'Rating': rating,
                               'Duration': duration, 'Description': description,
                               'Stars': stars, 'Directors': directors})

    pd.DataFrame(movie_data).to_csv("indonesian_movie_data_imdb.csv", index=False)

    return movie_data, total_movies


# fetch_imdb_data()

df = pd.read_csv("indonesian_movie_data_imdb.csv")
df = df.drop_duplicates()
df
