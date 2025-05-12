import gspread
import pandas as pd
import logging
from datetime import datetime
import utils
import os
import dotenv

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

    # Prepare variables: Hubspot Pull
    hs_api_key = os.environ["HUBSPOT_API_KEY"]
    list_number = 246
    url = f"https://api.hubapi.com/contacts/v1/lists/{list_number}/contacts/all?property=hs_linkedin_url&firstname&lastname&post_name&organic_social_outreached&outreach_note"
    properties=["organic_social_outreached", "outreach_note", "hs_linkedin_url", "firstname", "lastname", "post_name", ]

    # Pull list from Hubspt
    hubspot_engagers = utils.hubspot_fetch_list_contacts(hs_api_key, url, properties).rename(columns={"hs_linkedin_url": "linkedin_url"})
    logger.info(f"Got HubSpot Engagers (list #{list_number}):\n{hubspot_engagers}")

    #


if __name__ == "__main__":
    main()
