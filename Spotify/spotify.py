import requests
import pandas as pd
import google.cloud.bigquery as bigquery
from urllib.parse import urlencode
import base64
import webbrowser

SPOTIFY_SECRET = "BQBsLDsIfMLyBCZ32e59qAWWRelkBAuzZkmhE2nzZZTjzCQXsMPkoJBgl1GvhcqoFkVgYXIvtFk-FKa8Iq4r1y4SseI2IGsXXwCWsyGWyyk3nvd4AP7HHUO84GANxF-57eZ_3TXp0V2zmWL_Xwf_b2dwuzVKMb6N1EMHbuGAOTvjwqQTMYsSlR5RvyDu"
CLIENT_ID = "8854c9319ee34680bf08337f4846f0e7"
CLIENT_SECRET = "54df67eda8e9463bb6876c10a9f80716"

# usar authCode() para pegar o authorization code e em seguida colocar ele em validation() para gerar o token do spotify 

def authCode():
    client_id = CLIENT_ID

    auth_headers = {
    "client_id": client_id,
    "response_type": "code",
    "redirect_uri": "http://localhost/",
    "scope": "user-library-read"
    }

    webbrowser.open("https://accounts.spotify.com/authorize?" + urlencode(auth_headers))

def validation() -> bool:
    
    client_id = CLIENT_ID
    client_secret = CLIENT_SECRET   

    code = "AQDc1STcLdiM8UMbcHnOunjTnOStcdiYuxSk6sob9tXIrrY_4bK5NuXCGLNDyjaTqRaKMccHykI8RvD893apYNKH2MIEI3WpRZegEs-UgO1yvegW2iTMT8deymQYzROsSQNFnu6poLp47IG3pJ1TtUbnAxza7HRfuPxE5cXnz-tU2m5jJTDUmw"
    encoded_credentials = base64.b64encode(client_id.encode() + b':' + client_secret.encode()).decode("utf-8")

    token_headers = {
        "Authorization": "Basic " + encoded_credentials,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    token_data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": "http://localhost/"
    }

    r = requests.post("https://accounts.spotify.com/api/token", data=token_data, headers=token_headers)
    
    token = r.json()["access_token"]

    print(token)

    return True

def extract() -> bool:

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {SPOTIFY_SECRET}"
    }




    search = "data hackers"
    request = requests.get(f"https://api.spotify.com/v1/search?q=`{search}`&type=show&limit=50", headers = headers)

    show_data = request.json()

    print(show_data)

    show_id = []
    show_name = []
    show_description = []
    show_total_episodes = []

    for show in show_data["shows"]["items"]:
        show_id.append(show["id"])
        show_name.append(show["name"])
        show_description.append(show["description"])
        show_total_episodes.append(show["total_episodes"])

    show_dict = {
        "id" : show_id,
        "name" : show_name,
        "description" : show_description,
        "total_episodes" : show_total_episodes,
    }

    show_df = pd.DataFrame(show_dict, columns=["id", "name", "description", "total_episodes"])

    print(show_df)


    request = requests.get(f"https://api.spotify.com/v1/search?q=`{search}`&type=episode&limit=50", headers = headers)

    episode_data = request.json()

    episode_id = []
    epsiode_name = []
    epsiode_description = []
    episode_release_date = []
    episode_duration = []
    episode_language = []
    episode_explicit = []
    episode_type = []

    for episode in episode_data["episodes"]["items"]:
        episode_id.append(episode["id"])
        epsiode_name.append(episode["name"])
        epsiode_description.append(episode["id"])
        episode_release_date.append(episode["release_date"])
        episode_duration.append(episode["duration_ms"])
        episode_language.append(episode["language"])
        episode_explicit.append(episode["explicit"])
        episode_type.append(episode["type"])

    episode_dict = {
        "id" : episode_id,
        "name" : epsiode_name,
        "description" : epsiode_description,
        "release_date" : episode_release_date,
        "duration_ms" : episode_duration,
        "language" : episode_language,
        "explicit" : episode_explicit,
        "type" : episode_type
    }

    episode_df = pd.DataFrame(episode_dict, columns=["id", "name", "description", "release_date", "duration_ms", "language", "explicit", "type" ])

    print(episode_df)

    return True


validation()

