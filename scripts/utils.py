import requests
from datetime import datetime
import os
import pandas as pd
import logging

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