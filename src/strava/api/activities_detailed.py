import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import pandas as pd
import pyarrow
import json
import datetime
import time
import os
import glob
import readline
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv()

# Access the environment variables
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
refresh_token = os.getenv('REFRESH_TOKEN')

payload = {
    'client_id': client_id,
    'client_secret': client_secret,
    'refresh_token': refresh_token,
    'grant_type': "refresh_token",
    'f': 'json'
}

auth_url = "https://www.strava.com/oauth/token"
activites_url = "https://www.strava.com/api/v3/athlete/activities"

print("Requesting Token...\n")
res = requests.post(auth_url, data=payload, verify=False)
access_token = res.json()['access_token']

def write_dataframe_to_parquet(file_path, df):
    df.to_parquet(file_path, engine='pyarrow')

def activityid_list():
  """
  Returns a list of all activityId's for the current set-up
  """
  header = {'Authorization': 'Bearer ' + access_token}
  # The first loop, request_page_number will be set to one, so it requests the first page. Increment this number after
  # each request, so the next time we request the second page, then third, and so on...
  request_page_num = 1
  all_activities = []

  while True:
      param = {'per_page': 200, 'page': request_page_num}
      # initial request, where we request the first page of activities
      my_dataset = requests.get(activites_url, headers=header, params=param).json()

      # check the response to make sure it is not empty. If it is empty, that means there is no more data left. So if you have
      # 1000 activities, on the 6th request, where we request page 6, there would be no more data left, so we will break out of the loop
      if len(my_dataset) == 0:
          print("breaking out of while loop because the response is zero, which means there must be no more activities")
          break

      # if the all_activities list is already populated, that means we want to add additional data to it via extend.
      if all_activities:
          print("all_activities is populated")
          all_activities.extend(my_dataset)

      # if the all_activities is empty, this is the first time adding data so we just set it equal to my_dataset
      else:
          print("all_activities is NOT populated")
          all_activities = my_dataset

      request_page_num += 1

  print("Total activites in all_activities: " + str(len(all_activities)))
    
def all_activites_new(activityId):

    headers = {
                "accept": "application/json",
                "authorization": f"Bearer {access_token}"
        }

    url = f"https://www.strava.com/api/v3/activities/{activityId}?include_all_efforts=true"

    response = requests.get(url, headers=headers)
    response = response.json()
    return(response)
            
# Set the maximum request limit and the time interval
request_limit = 600 # I think
interval_minutes = 15
request_count = 0

# Initialize the result list
all_activites_new_list = []

# Function to make the API request
def make_api_request():
    # Make the API request here
    for i in range(0, len(activityId_list)):
        response = all_activites_new(activityId_list[i])
        all_activites_new_list.append(response)    
        global request_count
        request_count += 1           
        # If the request limit has been reached, wait for the specified interval and continue
        if request_count >= request_limit:
            current_time = datetime.now().time().strftime("%I:%M %p")
            print(f"Reached the request limit of {request_limit} for the current batch, we are at {i} requests.")
            print(f"Waiting for {interval_minutes} minutes. It is {current_time} when the waiting starts.")
            time.sleep(interval_minutes * 60)
            request_count = 0  # Reset the request count after the delay
    return(all_activites_new_list)

result = make_api_request()
result = pd.json_normalize(result)

current_date = datetime.date.today().strftime('%Y%m%d')
file_path = f'all_activities_detailed_clientId_{strava_config.client_id}_{current_date}.parquet'  # Specify the path and name of the file
write_dataframe_to_parquet(file_path, result)