from typing import Union
from letterboxdpy.core.scraper import InvalidResponseError
from letterboxdpy.movie import Movie
import csv
import time
from datetime import datetime

arrMovieData = []
failed_slugs = []

arrAllMovies = [];
lines = open("all-user-movies.txt").read().splitlines()
for line in lines:
    arrAllMovies.append(line)

current_date = datetime.now().strftime("%Y-%m-%d")
current_filename = "latest_data_" + current_date + ".csv"
current_filename_ratings = "latest_data_ratings_" + current_date + ".csv"

try:
    offset = 0
    for index, movie in enumerate(arrAllMovies[offset:]):
        try:
            movie_instance = Movie(movie)
            movie_data = {
                "slug": movie_instance.slug,
                "url": movie_instance.url,
                "title": movie_instance.title,
                "rating": movie_instance.rating,
                # "rating_counts": movie_instance.rating_counts,
                "tmdb_link": movie_instance.tmdb_link,
                "poster": movie_instance.poster,
                "banner": movie_instance.banner
            }
            arrMovieData.append(movie_data)
            print(f"{index + offset}: Movies fetched: {len(arrMovieData)} - {movie_instance.slug}")
        except Exception as movie_error:
            failed_slugs.append(movie)
            print(f"{index + offset}: FAILED - {movie}: {movie_error}")
            continue

    with open(current_filename, 'a') as csv_file:
        writer = csv.writer(csv_file)
        for item in arrMovieData:
            csv_row = [
                item.get("slug"),
                item.get("url"),
                item.get("title"),
                item.get("rating"),
                item.get("tmdb_link"),
                item.get("poster"),
                item.get("banner")]
            writer.writerow(csv_row)

    # with open(current_filename_ratings, 'a') as csv_file_ratings:
    #     writer = csv.writer(csv_file_ratings)
    #     writer.writerow(["film_slug", "rating", "rating_counts"])
    #     for item in arrMovieData:
    #         rating_counts = item.get("rating_counts")
    #         for rating, count in rating_counts.items():
    #             writer.writerow([item.get("slug"), rating, count])

    with open("failed_movies.txt", 'w') as failed_file:
        for slug in failed_slugs:
            failed_file.write(slug + "\n")
    print(f"Done. {len(arrMovieData)} fetched, {len(failed_slugs)} failed (see failed_movies.txt).")

except Exception as e:
    print(e)
    print("timestamp:", time.time())
    # write any movies fetched by this point to csv file
    with open(current_filename, 'a') as csv_file:
        writer = csv.writer(current_filename)
        for item in arrMovieData:
            csv_row = [
                item.get("slug"), 
                item.get("url"), 
                item.get("title"), 
                item.get("rating"), 
                # item.get("rating_counts"),
                item.get("tmdb_link"),
                item.get("poster"),
                item.get("banner")]
            writer.writerow(csv_row)

    # with open(current_filename_ratings, 'a') as csv_file_ratings:
    #     writer = csv.writer(csv_file_ratings)
    #     writer.writerow(["film_slug", "rating", "rating_counts"])
    #     for item in arrMovieData:
    #         rating_counts = item.get("rating_counts")
    #         for rating, count in rating_counts.items():
    #             writer.writerow([item.get("slug"), rating, count])

    with open("failed_movies.txt", 'w') as failed_file:
        for slug in failed_slugs:
            failed_file.write(slug + "\n")

