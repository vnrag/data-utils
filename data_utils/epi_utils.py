import base64


def create_api_key_string(epi_user, epi_password):
    api_key = '{epi_user}:{epi_password}'.format(epi_user=epi_user,
                                                 epi_password=epi_password)
    return base64.b64encode(api_key.encode())

