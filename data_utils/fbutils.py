import json
import urllib3


def create_token_url(app_id, app_secret):
    url = f"https://graph.facebook.com/oauth/access_token?client_id=" \
          f"{app_id}&client_secret={app_secret}&grant_type=" \
          f"client_credentials"
    return url


def create_url_data(start, stop, access_token, api_token):
    url = f'https://graph.facebook.com/v6.0/{api_token}/insights?pretty' \
          f'=0&metric=page_impressions&since={start}&until={stop}&' \
          f'access_token={access_token}'
    return url


def handle_get_request(url):
    http = urllib3.PoolManager()
    r = http.request('GET', url)
    if r.status != 200:
        print(r.data)
        raise SystemExit
    return json.loads(r.data.decode('utf-8'))