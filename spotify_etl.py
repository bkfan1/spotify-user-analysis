import spotipy
import pandas as pd

from sqlalchemy import create_engine
from spotipy.oauth2 import SpotifyOAuth

import json

from config import CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, SCOPE, TARGET_DATABASE_URL
from utils import music_pitches

# ETL functions related to user's top artists


def extract_user_top_artists():

    try:
        # Authentication with Spotify API
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=CLIENT_ID,
                                                       client_secret=CLIENT_SECRET, redirect_uri=REDIRECT_URI, scope=SCOPE))
        # Getting the user top artists data in a python dictionary format
        raw_data = sp.current_user_top_artists()

        # Creating a JSON file to save the raw stringyfied data
        with open("user_top_artists_RAW.json", "w") as json_file:
            # Converting the obtained raw data into JSON file
            content = json.dumps(raw_data)

            json_file.write(content)
            json_file.close()

    except Exception as e:
        raise e


def transform_user_top_artists():

    try:
        # Opening the JSON file who contains the raw user top artists data
        with open("user_top_artists_RAW.json", "r") as json_file:
            # Reading the content of the JSON file
            json_content = json_file.read()

            # Parsing the JSON file content to convert it in a python dictionary
            data_dict = json.loads(json_content)

            print(data_dict)

            # Since the important data its on "items" property, we are going to save
            # their value in a variable
            user_top_artists = data_dict["items"]

            print(user_top_artists)

            df = pd.DataFrame(data=user_top_artists)

            print(df)

            # Renaming "id" column to "spotify_id"
            df.rename(columns={"id": "spotify_id"}, inplace=True)

            # Selecting only the useful columns to EDA
            transformed_df = df[["spotify_id", "name", "popularity"]]

            # Saving the transformed data in a CSV file
            transformed_df.to_csv(
                "user_top_artists_CLEANED.csv", ",", index=False)

            json_file.close()

    except Exception as e:
        raise e


def load_user_top_artists():

    try:
        # Reading the user top artists cleaned data from CSV file
        # And saving it in a DataFrame
        df = pd.read_csv("user_top_artists_CLEANED.csv")

        # Creating an engine to connect to target database
        engine = create_engine(url=TARGET_DATABASE_URL)

        # Uploading the cleaned data to target database
        df.to_sql(name="user_top_artists",
                  con=engine, if_exists="replace", index=False)

    except Exception as e:
        raise e


# ETL functions related to user's top songs

def extract_user_top_tracks():
    try:
        # Authentication with Spotify API
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=CLIENT_ID,
                                                       client_secret=CLIENT_SECRET, redirect_uri=REDIRECT_URI, scope=SCOPE))

        # Getting the user top tracks in python dictionary format
        raw_data = sp.current_user_top_tracks()

        # Creating a JSON file to store raw user top tracks data
        with open("user_top_tracks_RAW.json", "w") as json_file:
            # Stringyfing the raw data obtained from current_user_top_tracks()
            content = json.dumps(raw_data)

            json_file.write(content)

            json_file.close()

    except Exception as e:
        raise e


def transform_user_top_tracks():
    try:
        # Opening the JSON file that contains the raw user top tracks data
        with open("user_top_tracks_RAW.json", "r") as json_file:
            # Reading the JSON file content
            json_content = json_file.read()

            # Parsing the JSON file content
            data_dict = json.loads(json_content)

            print(data_dict)

            # Since the useful data its on "items" attribute, we are going to extract it only
            user_top_tracks = data_dict["items"]

            print(user_top_tracks)

            tracks_df = pd.DataFrame(data=user_top_tracks)

            # Selecting only the useful columns from the DataFrame
            user_top_tracks_df = tracks_df[["id", "name", "popularity"]]

            # Renaming "id" column to "track_id", since "id" its the identifier
            # of the track on Spotify
            user_top_tracks_df.rename(columns={"id": "track_id"}, inplace=True)

            # Getting the track ids and saving them in a list
            track_ids = user_top_tracks_df["track_id"].tolist()

            # Authenticating in Spotify
            sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=CLIENT_ID,
                                                           client_secret=CLIENT_SECRET, redirect_uri=REDIRECT_URI, scope=SCOPE))

            # Using the tracks_id list to get audio features for each
            # user top track
            audio_features = sp.audio_features(tracks=track_ids)

            print(audio_features)

            # Since spotify provide us the key of the track in pitch class notation
            # We have to transform it into their tonal counterparts for EDA purposes
            for track_features in audio_features:
                track_key = track_features["key"]

                key_in_tone = music_pitches[track_key]

                track_features["key"] = key_in_tone

            # Converting the raw audio features data from user top tracks in a DataFrame
            audio_features_df = pd.DataFrame(data=audio_features)

            print(audio_features_df)

            # Selecting only the needed columns
            analysis_df = audio_features_df[["duration_ms", "key", "tempo"]]

            # Concatinating user_top_tracks_df and analysis_df
            combined_df = pd.concat([user_top_tracks_df, analysis_df], axis=1,)

            # Saving the combined df data into a CSV file
            combined_df.to_csv("user_top_tracks_CLEANED.csv", ",", index=False)

            json_file.close()

    except Exception as e:
        raise e


def load_user_top_tracks():
    try:
        # Loading the user top tracks data from cleaned CSV file
        user_top_tracks_df = pd.read_csv("user_top_tracks_CLEANED.csv")

        # Creating a SQLAlchemy engine to connect to target database
        engine = create_engine(url=TARGET_DATABASE_URL)

        # Loading the user top tracks to target database
        user_top_tracks_df.to_sql(name="user_top_tracks",
                                  con=engine, if_exists="replace", index=False)
    except Exception as e:
        raise e


def run_user_top_artists_etl():
    try:
        extract_user_top_artists()
        transform_user_top_artists()
        load_user_top_artists()

    except Exception as e:
        raise e


def run_user_top_tracks_etl():
    try:
        extract_user_top_tracks()
        transform_user_top_tracks()
        load_user_top_tracks()

    except Exception as e:
        raise e


run_user_top_artists_etl()
run_user_top_tracks_etl()
