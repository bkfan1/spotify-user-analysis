import spotipy
import pandas as pd

from sqlalchemy import create_engine
from spotipy.oauth2 import SpotifyOAuth

from config import SCOPE, CLIENT_ID, CLIENT_SECRET, REDIRECT_URI

from utils.uuid_generator import generate_short_uuid
from utils.logging import log_exception


def extract():

    try:
        # Authentication with Spotify API
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=CLIENT_ID,
                                                       client_secret=CLIENT_SECRET, redirect_uri=REDIRECT_URI, scope=SCOPE))
        raw_data = sp.current_user_top_artists()
        return raw_data
    except Exception as e:
        log_exception("Error while extracting the data:", e)
        return None


def transform(raw_data):

    try:
        user_top_artists = raw_data["items"]

        artists_dict = {
            "id": [],
            "artist_name": []
        }

        genres_dict = {
            "id": [],
            "genre_name": []
        }

        artists_genres_dict = {
            "genre_id": [],
            "artist_id": []
        }

        for artist in user_top_artists:

            artist_id = generate_short_uuid()

            artists_dict["id"].append(artist_id)
            artists_dict["artist_name"].append(artist["name"])

            for genre in artist["genres"]:
                if genre not in genres_dict["genre_name"]:
                    genre_id = generate_short_uuid()

                    genres_dict["id"].append(genre_id)
                    genres_dict["genre_name"].append(genre)

                    artists_genres_dict["artist_id"].append(artist_id)
                    artists_genres_dict["genre_id"].append(genre_id)

        artists_df = pd.DataFrame(data=artists_dict)
        genres_df = pd.DataFrame(data=genres_dict)
        artists_genres_df = pd.DataFrame(data=artists_genres_dict)

        data = [
            {
                "name": "user_top_artists",
                "df": artists_df,
            },
            {"name": "user_genres", "df": genres_df},
            {"name": "user_top_artists_genres", "df": artists_genres_df}
        ]

        return data
    except Exception as e:
        log_exception("Error while transforming the data:", e)


def load(transformed_data):

    try:
        database_url = "sqlite:///user_data_spotify.db"
        engine = create_engine(database_url)

        for table_dict in transformed_data:
            table_name = table_dict["name"]

            table_dict["df"].to_sql(
                table_name, engine, if_exists="replace", index=False)

        return True
    except Exception as e:
        log_exception("Error while loading the data:", e)
        return None
