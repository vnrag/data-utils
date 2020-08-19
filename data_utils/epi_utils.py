import base64
import json


def create_api_key_string(epi_user, epi_password):
    """Creates api key string for the epi server

    Arguments:
        epi_user {[string]} -- [User for epi server]
        epi_password {[string]} -- [Password for epi password]

    Returns:
        [string] -- [Encrypted key]
    """
    api_key = '{epi_user}:{epi_password}'.format(epi_user=epi_user,
                                                 epi_password=epi_password)
    return base64.b64encode(api_key.encode()).decode('utf-8')


def get_folders(folders_json):
    """Gets folder elements from given json.

    Arguments:
        folders_json {[json]} -- [Folder with json]

    Returns:
        [list] -- [List of json]
    """
    try:
        folders = []
        for folder in folders_json['elements']:
            del folder['links']
            folders.append(folder)
        return folders
    except Exception as e:
        print(e)
        return None


def get_campaign_ids(campaigns_json):
    """Gets campaign ids from given json.

    Arguments:
        campaigns_json {[json]} -- [List of campaigns]

    Returns:
        [list] -- [List of campaign ids]
    """
    campaign_ids = []
    if 'elements' in campaigns_json:
        for element in campaigns_json['elements']:
            if 'id' in element:
                campaign_ids.append(json.dumps(element['id']))
        return campaign_ids
    else:
        return None


def get_campaign_report(campaign_report_json):
    """Creates the needed report from the given json

    Arguments:
        campaign_report_json {[json]} -- [Json from campaign report]

    Returns:
        [dict] -- [Needed information in json format for campaign report]
    """
    try:
        campaign_report_json = dict(campaign_report_json)
        del campaign_report_json['links']
        del campaign_report_json['messages']
        return campaign_report_json
    except:
        return None


def get_mailing_reports(campaign_report_json):
    """Creates the needed report from the given json

    Arguments:
        campaign_report_json {[json]} -- [Campaign's json value]

    Returns:
        [dict] -- [Gets messages from given json]
    """
    try:
        campaign_report_json = dict(campaign_report_json)
        messages = campaign_report_json['messages']
        return messages
    except:
        return None


def get_campaign(campaign_json):
    """Get campaign values

    Arguments:
        campaign_json {[json]} -- [Campaign json from API]

    Returns:
        [json] -- [Needed info from campaign json]
    """
    try:
        campaign_json = dict(campaign_json)
        del campaign_json['links']
        del campaign_json['messages']
        del campaign_json['recipientLists']['links']
        del campaign_json['recipientLists']['gridLocation']
        del campaign_json['targetGroups']
        return campaign_json
    except:
        return None


def get_mailings(campaign_json):
    """Gets only needed information from campaign json for mailings

    Arguments:
        campaign_json {[json]} -- [Campaign json value from API]

    Returns:
        [json] -- [Mailings informations]
    """
    try:
        campaign_json = dict(campaign_json)
        mailings = campaign_json['messages']
        for i in range(len(mailings)):
            if 'links' in mailings[i]:
                del mailings[i]['links']
            if 'gridLocation' in mailings[i]:
                del mailings[i]['gridLocation']
        return mailings
    except:
        return None


def get_folder_ids(folder_json):
    """Gets ids for given folder json

    Arguments:
        folder_json {[json]} -- [Json from API]

    Returns:
        [list] -- [List of json]
    """
    try:
        folders = []
        for i in range(len(folder_json['elements'])):
            folders.append(json.dumps(int(folder_json['elements'][i]['id'])))
        return folders
    except:
        return None


def map_server_folder(df):
    df['id'] = df['id'].astype('int64')
    df['name'] = df['name'].astype('str')
    df['parentId'] = df['parentId'].astype('int64')
    df['childrenIds'] = df['childrenIds'].astype('object')
    df['created'] = df['created'].astype('str')
    df['client_id'] = df['client_id'].astype('str')
    return df


def adjust_epi_blacklist_json_reason(blacklist_json):
    """Adjusts the value of the reason string so that each reason can be split
    
    Arguments:
        blacklist_json {[json]} -- [List of blacklists]

    Returns:
        [json] -- [Adjusted blacklist json]
    """
    for blacklist in blacklist_json['elements']:
        blacklist['reason']= dict(r.split("=")  \
            for r in blacklist['reason'].split(";"))
    return blacklist_json


def check_epi_blaclist_reason_columns(blacklist_df):
    """Checks that there are no missing reason columns columns
    
    Arguments:
        blacklist_df {[pandas dataframe]} -- [List of blacklists]

    Returns:
        [pandas dataframe] -- [Adjusted blacklist dataframe]
    """
    reasons_col = ['reason.type','reason.rule','reason.mailing','reason.m2u']
    for col in reasons_col:
        if col not in blacklist_df.columns:
            blacklist_df[col] = None
    return blacklist_df