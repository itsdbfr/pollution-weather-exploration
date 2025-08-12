import requests
import time
import pandas as pd
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed

############################################################
############################################################

#EPA DATA RETRIEVAL

#EPA Credentials

epaEmail = 'daksh.bhh@gmail.com'
epaKey = 'bluecat55'

#EPA Status Check

epaStatusCheck = requests.get("https://aqs.epa.gov/data/api/metaData/isAvailable")
print(epaStatusCheck.text)

#Identified and compiled a list of pollution variables, represented by parameter codes

priorityParameterCodes = {
    'Ozone': '44201',
    'PM25 - Local Conditions': '88101',
    'Carbon monoxide': '42101'
}

#Retrieve relevant lists of codes in JSON and convert them into a dataframe for ease of use to retrieve other relevant data

def APIcodeRequest(serviceLink, requestName):
    request = requests.get(serviceLink)
    print(request.text)
    requestData = request.json()
    requestDataDictionary = {}
    for item in requestData['Data']:
        requestDataDictionary[str(item['value_represented'])] = str(item['code'])  
    request_df = pd.DataFrame(list(requestDataDictionary.items()), columns=['name', 'code'])
    request_df.to_parquet(f"/Users/db/Desktop/pollution-weather-exploration/Data/{requestName}.parquet")
    print(f"\n{requestName}.parquet was created and saved.")

#Retrieving parameter codes and state codes
    
APIcodeRequest("https://aqs.epa.gov/data/api/list/parametersByClass?email=test@aqs.api&key=test&pc=CRITERIA", "EPA Parameter Codes")
APIcodeRequest("https://aqs.epa.gov/data/api/list/states?email=test@aqs.api&key=test", "EPA State Codes")

#Reading state codes from saved parquet file and iterating through every state to compile all county codes for each state

stateCode_df = pd.read_parquet("/Users/db/Desktop/pollution-weather-exploration/Data/EPA State Codes.parquet")

# Filter out non-numeric codes and codes > 56 (Wyoming's code)

stateCode_df = stateCode_df[stateCode_df['code'].str.match(r'^\d+$')]
stateCode_df = stateCode_df[stateCode_df['code'].astype(int) <= 56] 

for index, row in stateCode_df.iterrows():
    request = requests.get(f"https://aqs.epa.gov/data/api/list/countiesByState?email=test@aqs.api&key=test&state={row['code']}")
    print(request.text)
    requestData = request.json()
    requestDataDictionary = {}
    for item in requestData['Data']:
        requestDataDictionary[str(item['value_represented'])] = str(item['code'])
    request_df = pd.DataFrame(list(requestDataDictionary.items()), columns=['name', 'code'])
    request_df['stateCode'] = row['code']
    request_df.to_parquet(f"/Users/db/Desktop/pollution-weather-exploration/Data/EPA {row['name']} County Codes.parquet")
    print(f"\nEPA {row['name']} County Codes.parquet was created and saved.")

#Combining all parquet files into a singular county code file

def compileParquetFiles(filePath, destinationPath):
    files = glob.glob(filePath)
    info_dfs = []
    for file in files:
        file_df = pd.read_parquet(file)
        info_dfs.append(file_df)
    combinedFiles_df = pd.concat(info_dfs, ignore_index=True)
    combinedFiles_df.to_parquet(destinationPath)
    print(f"Files combined and printed at {destinationPath}")
    return None

#needed unique naming and handling outside of the function for this situation so did not use the function
    
countyFiles = glob.glob("/Users/db/Desktop/pollution-weather-exploration/Data/EPA *County Codes.parquet")
allCounty_dfs = []
for file in countyFiles:
    file_df = pd.read_parquet(file)
    stateName = file.split("EPA ")[1].split(" County Codes.parquet")[0]
    file_df['stateName'] = stateName
    allCounty_dfs.append(file_df)
combinedCountyCodes_df = pd.concat(allCounty_dfs, ignore_index=True)
combinedCountyCodes_df.to_parquet("/Users/db/Desktop/pollution-weather-exploration/Data/EPA All Counties Combined.parquet")
print(f"Combined {len(countyFiles)} files into one. Total counties: {len(combinedCountyCodes_df)}")
print("Combined dataframe shape:", combinedCountyCodes_df.shape)
print("Column names:", combinedCountyCodes_df.columns.tolist())
print("First few rows:")
print(combinedCountyCodes_df.head())

#creating a single task function to allow for concurrent processing

def singleRequest(paramName, paramCode, stateName, stateCode, countyName, countyCode):
    epaDataRequest = requests.get(f"https://aqs.epa.gov/data/api/annualData/byCounty?email={epaEmail}&key={epaKey}&param={paramCode}&bdate=20200101&edate=20201231&state={stateCode}&county={countyCode}")
    print(epaDataRequest.status_code)
    if epaDataRequest.status_code == 200:
        countyData = epaDataRequest.json()
        if countyData['Data']:
            countyData_df = pd.json_normalize(countyData['Data'])
            countyData_df['Parameter'] = paramName
            countyData_df['State'] = stateName
            countyData_df['County'] = countyName
            time.sleep(0.1) 
            return countyData_df
        else:
            print("No data retrieved")
            time.sleep(0.1) 
            return None 
    else:
        print(f"For county: {countyName} in state: {stateName} and parameter: {paramName} -")
        print("Request Failed")
        time.sleep(0.1) 
        return None

#creating the concurrent processing function
    
def batchProcessing(countiesProcessed, batchNumber):
    requestDataList = []
    for index, row in countiesProcessed.iterrows():
        print(row['name'])
        for parameter, paramCode in priorityParameterCodes.items():
            requestDataList.append((parameter, paramCode, row['stateName'], row['stateCode'], row['name'], row['code']))
    countyDataList = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        futureToRequest = {executor.submit(singleRequest, req[0], req[1], req[2], req[3], req[4], req[5]): req for req in requestDataList}
        for future in as_completed(futureToRequest):
            requestInfo = futureToRequest[future]
            try:
                result = future.result()    
                if result is not None:
                    countyDataList.append(result)        
            except Exception as e:
                print(f"Error processing request: {e}")     
    if countyDataList:
        countyBatchDataCombined_df = pd.concat(countyDataList, ignore_index = True)
        countyBatchDataCombined_df.to_parquet(f"/Users/db/Desktop/pollution-weather-exploration/Data/statedatabatch_0{batchNumber}.parquet")
        print(f"Batch Number {batchNumber} printed to parquet")
    else:
        print(f"Batch Number {batchNumber}: No data collected")

#processing in batches of 50 counties at a time

for i in range(0, len(combinedCountyCodes_df), 50):
    batch_df = combinedCountyCodes_df[i:i+50]
    batchNumber = (i//50)+1
    print(f"Retrieving data for batch number {batchNumber}.")
    batchProcessing(batch_df, batchNumber)

compileParquetFiles("/Users/db/Desktop/pollution-weather-exploration/Data/statedatabatch_0*.parquet", "/Users/db/Desktop/pollution-weather-exploration/Data/stateDataBatchCombined.parquet")

############################################################
############################################################
    
from google.cloud import bigquery as bq
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

#BIGQUERY DATA RETRIEVAL
  
client = bq.Client(project='pollution-weather-exploration')

#writing SQL queries to grab desired data

weatherQuery = """
SELECT *
FROM `bigquery-public-data.noaa_gsod.gsod2020` 
"""

weatherStationsQuery = """
SELECT usaf, wban, name, lat, lon, country, state
FROM `bigquery-public-data.noaa_gsod.stations`
WHERE country = "US"
"""

#geo_id comprises of FIPS codes
censusQuery = """
SELECT
    geo_id,
    aggregate_travel_time_to_work,
    bachelors_degree_or_higher_25_64,
    civilian_labor_force,
    commuters_by_carpool,
    commuters_by_public_transportation,
    families_with_young_children,
    households_public_asst_or_food_stamps,
    income_200000_or_more,
    income_less_10000,
    income_per_capita,
    unemployed_pop
FROM `bigquery-public-data.census_bureau_acs.county_2020_5yr`
"""

#Commented out query retrieval - referring to parquet files after

weather_df = client.query(weatherQuery).to_dataframe()
weatherStations_df = client.query(weatherStationsQuery).to_dataframe()
census_df = client.query(censusQuery).to_dataframe()
print(weather_df.head(10))
print(census_df.head(10))

weather_df.to_parquet("/Users/db/Desktop/pollution-weather-exploration/Data/weather_data.parquet")
weatherStations_df.to_parquet("/Users/db/Desktop/pollution-weather-exploration/Data/weatherStations_data.parquet")
census_df.to_parquet("/Users/db/Desktop/pollution-weather-exploration/Data/census_data.parquet")

#referring to parquet files and re-converting back to df to limit API calls

weatherStations_df = pd.read_parquet("/Users/db/Desktop/pollution-weather-exploration/Data/weatherStations_data.parquet")
print(len(weatherStations_df))

#using geolocator to reconcile coordinates of weather stations with county boundaries

geolocator = Nominatim(user_agent="pollutionAnalysis", timeout=10)
geocode = RateLimiter(geolocator.reverse, min_delay_seconds=2)

def reconcileCoordinates(latList, lonList):
    countyList = []
    for lat, lon in zip(latList, lonList):
        try:
            location = geocode((float(lat), float(lon)), exactly_one=True)
            county = (location.raw['address'].get('county', 'No county found'))    
        except Exception as e:
            print(f"Error: {e}\nlatitude: {lat}\nlongitude: {lon}")
            county = f"Error: {e}, latitude: {lat}, longitude: {lon} "
        countyList.append(county)
        
    return countyList

stationBatchSize = 100
stationLatList = weatherStations_df['lat'].tolist()
stationLonList = weatherStations_df['lon'].tolist()

#adding county label to stations in batches of 100 stations

for i in range(0, len(weatherStations_df), stationBatchSize):
    stationBatchNumber = (i//stationBatchSize)+1
    stationLatListBatch = stationLatList[i:i+stationBatchSize]
    stationLonListBatch = stationLonList[i:i+stationBatchSize]
    
    print(f"Processing batch number 0{stationBatchNumber}")
    labelList = reconcileCoordinates(stationLatListBatch, stationLonListBatch)
    weatherStations_df.loc[i:i+stationBatchSize-1, 'County'] = labelList
    batch_df = weatherStations_df.iloc[i:i + stationBatchSize]
    batch_df.to_parquet(f"/Users/db/Desktop/pollution-weather-exploration/Data/stationCountyBatch{stationBatchNumber}.parquet")
    print(f"batch number 0{stationBatchNumber} completed and exported to Parquet")
   
compileParquetFiles("/Users/db/Desktop/pollution-weather-exploration/Data/stationCountyBatch*.parquet", "/Users/db/Desktop/pollution-weather-exploration/Data/stationCountyBatchCombined.parquet")
