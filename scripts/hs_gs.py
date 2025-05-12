import gspread
import pandas as pd
import logging
from datetime import datetime
import utils
import os
import dotenv
import base64
import json

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

    # Pull remote engagers from HubSpot Organic Social list
    hs_api_key = os.environ["HUBSPOT_API_KEY"]
    list_number = 246
    logger.info("Pulling Hubspot List")
    url = f"https://api.hubapi.com/contacts/v1/lists/{list_number}/contacts/all?property=organic_social_outreached&property=hs_linkedin_url&property=firstname&property=lastname&property=company&property=phantombuster_linkedin_headline&property=post_name&property=reaction_type"
    properties=["organic_social_outreached", "hs_linkedin_url", "firstname", "lastname", "company", "phantombuster_linkedin_headline", "post_name", "reaction_type"]
    hubspot_engagers = utils.hubspot_fetch_list_contacts(hs_api_key, url, properties).rename(columns={"hs_linkedin_url": "linkedin_url"})
    logger.info(f"\nGot HubSpot Engagers (list #{list_number}):\n{hubspot_engagers}\n")

    # Filter list
    non_outreached = hubspot_engagers[hubspot_engagers["organic_social_outreached"].isna()]
    logger.info(f"Subset -- engagers to outreach to:\n{non_outreached}\n")

    # Pull Google Sheets Leads
    ss_name = "Organic Social Pipeline: Outreach Approvals"
    ws_name = "Leads"
    logger.info(f"Pulling info from Spreadsheet: {ss_name}, Worksheet: {ws_name}")
    encoded_key = str(os.getenv("SERVICE_ACCOUNT_KEY"))[2:-1]
    gs_leads = utils.gs_to_df(encoded_key, ss_name, ws_name)
    logger.info(f"\nGot Google Sheets Engagers):\n{gs_leads}\n")

    
    # Push leads to Organic Social List



if __name__ == "__main__":
    main()
