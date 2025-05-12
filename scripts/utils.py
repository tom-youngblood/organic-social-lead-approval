import requests
from datetime import datetime
import os
import pandas as pd
import logging
import gspread
import json
from google.oauth2.service_account import Credentials
import ast
import base64

logger = logging.getLogger(__name__)

def hubspot_fetch_list_contacts(api_key, url, properties):
    """
    Inputs: 
    Hubspot API Key (STR),
    URL (Str): URL of list 
    Properties (list): All properties to be returned
    
    Returns: 
    DF: list of contacts.
    """
    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    contacts = []
    params = {
        "count": 100
    }

    while True:
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Error: {response.status_code}, {response.text}")
            break

        data = response.json()

        # Extract contacts and add to the list
        if "contacts" in data:
            contacts.extend(data["contacts"])

        # Check for pagination (if there are more contacts to fetch)
        if "vid-offset" in data and data.get("has-more", False):
            params["vidOffset"] = data["vid-offset"] 
        else:
            break

    contacts_list = []

    for contact in contacts:
        parsed_data = {"vid": contact.get("vid")}  # Extract vid

        # Extract all requested properties, setting None if missing
        if "properties" in contact:
            for prop in properties:
                if prop in contact["properties"]:
                    parsed_data[prop] = contact["properties"][prop].get("value")
                else:
                    parsed_data[prop] = None  # Ensures consistency across all rows

        contacts_list.append(parsed_data)  # Add to list

    return pd.DataFrame(contacts_list)

def hubspot_push_contacts_to_list(api_key, df, properties_map):
    """Pushes all contacts from a DataFrame to HubSpot using a mapping of local column names to HubSpot property names."""
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    if len(df) == 0:
        logger.info("No new leads pushed")
        return

    for _, row in df.iterrows():
        # Build the properties dict using the mapping
        hubspot_properties = {}
        for local_col, hs_col in properties_map.items():
            value = row.get(local_col, "")
            # Convert NaN to empty string
            if pd.isnull(value):
                value = ""
            hubspot_properties[hs_col] = value

        hubspot_record = {"properties": hubspot_properties}

        response = requests.post(url, headers=headers, json=hubspot_record)

        if response.status_code == 201:
            logger.info(f"Successfully pushed: {row.get('name', row.get('firstname', ''))}")
        else:
            logger.error(f"Failed to push: {row.get('name', row.get('firstname', ''))}, Error: {response.text}")

    return None

def gs_to_df(encoded_key, ss_name, ws_name):
    """
    Arguments
    service_account_key (str): str(os.getenv("SERVICE_ACCOUNT_KEY"))[2:-1]
    ss_name (str): name of spreadsheet
    we_name (str): name of worksheet

    Returns
    links (df): dataframe representation of leads
    """
    # Get the encoded key from environment variable
    encoded_key = encoded_key
    logger.debug("Retrieved encoded service account key")

    # Decode the key
    gspread_credentials = json.loads(base64.b64decode(encoded_key).decode('utf-8'))
    logger.debug("Decoded service account key")

    # Connect to Google Sheets
    logger.info("Connecting to Google Sheets")
    try:
        gc = gspread.service_account_from_dict(gspread_credentials)
        data = gc.open(ss_name).worksheet(ws_name).get_all_values()
        links = pd.DataFrame(data[1:], columns=data[0])
        logger.info(f"Retrieved {len(links)} links from Google Sheets")
    except Exception as e:
        logger.error(f"Failed to connect to Google Sheets: {str(e)}")
        raise

    # Convert to DF, return
    return links
