from utils import hubspot_fetch_list_contacts
import os
from dotenv import load_dotenv

load_dotenv()
    
# Pull remote engagers from HubSpot Organic Social list
hs_api_key = os.environ["HUBSPOT_API_KEY"]
list_number = 246
url = f"https://api.hubapi.com/contacts/v1/lists/{list_number}/contacts/all?property=hs_linkedin_url&property=firstname&property=lastname"
properties=["hs_linkedin_url", "firstname", "lastname"]
hubspot_engagers = hubspot_fetch_list_contacts(hs_api_key, url, properties).rename(columns={"hs_linkedin_url": "linkedin_url"})

# Add count of duplicates
hubspot_engagers['duplicate_count'] = hubspot_engagers.groupby('linkedin_url')['linkedin_url'].transform('count')

# Get only the duplicates
duplicates = hubspot_engagers[hubspot_engagers.duplicated(subset=['linkedin_url'], keep=False)]

# Sort by duplicate count and LinkedIn URL for better readability
duplicates = duplicates.sort_values(['duplicate_count', 'linkedin_url'], ascending=[False, True])

# Print results
print(f"Duplicates:\n {duplicates}")
duplicates.to_csv("../data/temp_data/duplicates.csv")