import gspread
import pandas as pd
import logging
from datetime import datetime
import utils
import os
import dotenv
import base64
import json
import time

def main():
    # Configure logging
    logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/hs_gs_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)

    # Loading environment
    dotenv.load_dotenv()
    logger.info("Environment loaded")

    # Get the encoded key from environment variable
    encoded_key = str(os.getenv("SERVICE_ACCOUNT_KEY"))[2:-1]
    logger.debug("Retrieved encoded service account key")

    # Decode the key
    gspread_credentials = json.loads(base64.b64decode(encoded_key).decode('utf-8'))
    logger.debug("Decoded service account key")

    # Connect to Google Sheets
    logger.info("Connecting to Google Sheets")
    try:
        gc = gspread.service_account_from_dict(gspread_credentials)
        logger.info("Successfully connected to Google Sheets")
    except Exception as e:
        logger.error(f"Failed to connect to Google Sheets: {str(e)}")
        raise

    # Pull Google Sheets Leads
    ss_name = "Organic Social Pipeline: Outreach Approvals"
    ws_name = "Leads"
    logger.info(f"Pulling info from Spreadsheet: {ss_name}, Worksheet: {ws_name}")
    try:
        worksheet = gc.open(ss_name).worksheet(ws_name)
        gs_leads = pd.DataFrame(worksheet.get_all_records())
        logger.info(f"\nGot Google Sheets Engagers):\n{gs_leads}\n")
        if gs_leads.empty or not set(['vid', 'organic_social_outreached']).issubset(gs_leads.columns):
            logger.info("Google Sheet is empty or missing expected columns. Re-creating dataframe.")
            gs_leads = pd.DataFrame(columns=["vid", "organic_social_outreached", "hs_linkedin_url", "firstname", "lastname", "company", "phantombuster_linkedin_headline", "post_name", "reaction_type"])

        else:
            logger.info(f"\nGoogle Sheets engagers with their stages:\n{gs_leads[['vid', 'organic_social_outreached']]}")
    except Exception as e:
        logger.error(f"Failed to fetch data from Google Sheets: {str(e)}")
        raise

    # Pull remote engagers from HubSpot Organic Social list
    hs_api_key = os.environ["HUBSPOT_API_KEY"]
    list_number = 246
    logger.info("Pulling Hubspot list:")
    url = f"https://api.hubapi.com/contacts/v1/lists/{list_number}/contacts/all?property=organic_social_outreached&property=hs_linkedin_url&property=firstname&property=lastname&property=company&property=phantombuster_linkedin_headline&property=post_name&property=reaction_type"
    properties=["organic_social_outreached", "hs_linkedin_url", "firstname", "lastname", "company", "phantombuster_linkedin_headline", "post_name", "reaction_type"]
    hubspot_engagers = utils.hubspot_fetch_list_contacts(hs_api_key, url, properties)
    logger.info(f"\nGot HubSpot Engagers (list #{list_number}):\n{hubspot_engagers}\n")

    # Get hubspot engagers that are to be updated
    hubspot_engagers_to_be_updated = hubspot_engagers[~hubspot_engagers["organic_social_outreached"].str.lower().isin(["yes", "no"])]
    logger.info(f"Hubspot engagers to be updated:\n{hubspot_engagers_to_be_updated}")

    # Get GS leads that have been updated with valid values
    updated_leads = gs_leads[gs_leads["organic_social_outreached"].str.lower().isin(["yes", "no"])]
    # Convert values to proper case for HubSpot
    updated_leads["organic_social_outreached"] = updated_leads["organic_social_outreached"].str.lower().map({"yes": "Yes", "no": "No"})
    logger.info(f"Leads in Google Sheet that were recently updated with valid values (Yes/No):\n{updated_leads}\n)")

    # Update GS Leads -- Push changes to HubSpot
    if not updated_leads.empty:
        logger.info("Updating HubSpot leads with new stages...")
        url = f"https://api.hubapi.com/contacts/v1/lists/{list_number}"
        utils.hubspot_bulk_update_property(hs_api_key, url, updated_leads, "organic_social_outreached")
    else:
        logger.info("No leads to update in HubSpot")

    logger.info(f"Waiting 5 seconds for HS to refresh.")
    time.sleep(5)

    # Get leads that need to be in the Google Sheet (those with empty organic_social_outreached)
    # Exclude leads that were just updated in HubSpot
    leads_for_sheet = hubspot_engagers[
        ((hubspot_engagers["organic_social_outreached"].isna()) | 
        (hubspot_engagers["organic_social_outreached"] == "") |
        (hubspot_engagers["organic_social_outreached"] == "None") |
        (~hubspot_engagers["organic_social_outreached"].isin(["Yes", "No"]))) &
        (~hubspot_engagers["vid"].isin(updated_leads["vid"]))
    ]
    logger.info(f"Leads that need to be in Google Sheet (empty or invalid organic_social_outreached, excluding just updated):\n{leads_for_sheet}\n")

    # Replace GS leads with list ^
    try:
        # Clear existing content
        worksheet.clear()
        
        # Write headers
        worksheet.update(values=[leads_for_sheet.columns.tolist()], range_name='A1')
        
        # Write data
        if not leads_for_sheet.empty:
            worksheet.update(values=leads_for_sheet.values.tolist(), range_name='A2')
        
        logger.info("Successfully updated Google Sheet with leads that need staging")
    except Exception as e:
        logger.error(f"Failed to update Google Sheet: {str(e)}")
        raise

    logger.info("Script completed successfully!")

if __name__ == "__main__":
    main()
