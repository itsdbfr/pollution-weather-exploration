import pandas as pd
import numpy as np
import db_dtypes
from geopy.distance import geodesic


def cleanCountyName(countyName):
    if pd.isna(countyName):
        return countyName
    countyName = countyName.lower()
    suffixes = [' county', ' parish', ' borough', ' census Area', ' city and borough', ' city']
    for suffix in suffixes:
        if countyName.endswith(suffix):
            return countyName[:-len(suffix)].strip()
    return countyName.strip()

def cleanGeoID(geo_id):
    if pd.isna(geo_id):
        return str(geo_id)
    if len(geo_id) < 5:
        geo_id = '0'+ str(geo_id)
        return str(geo_id.strip())
    return str(geo_id.strip())

stateAbbreviationToFull = {
    'AL': 'Alabama',
    'AK': 'Alaska', 
    'AZ': 'Arizona',
    'AR': 'Arkansas',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'HI': 'Hawaii',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'IA': 'Iowa',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'ME': 'Maine',
    'MD': 'Maryland',
    'MA': 'Massachusetts',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MS': 'Mississippi',
    'MO': 'Missouri',
    'MT': 'Montana',
    'NE': 'Nebraska',
    'NV': 'Nevada',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NY': 'New York',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VT': 'Vermont',
    'VA': 'Virginia',
    'WA': 'Washington',
    'WV': 'West Virginia',
    'WI': 'Wisconsin',
    'WY': 'Wyoming'
}

stateFullToAbbreviation = {v: k for k, v in stateAbbreviationToFull.items()}

#Reading all data from parquet files

weather_df = pd.read_parquet("/Users/db/Desktop/pollution-weather-exploration/Data/weather_data.parquet")
weatherStations_df = pd.read_parquet("/Users/db/Desktop/pollution-weather-exploration/Data/stationCountyBatchCombined.parquet")
pollution_df = pd.read_parquet("/Users/db/Desktop/pollution-weather-exploration/Data/stateDataBatchCombined.parquet")
census_df = pd.read_parquet("/Users/db/Desktop/pollution-weather-exploration/Data/census_data.parquet")
fips_df = pd.read_csv("/Users/db/Desktop/pollution-weather-exploration/Data/fips_lookup_full.csv")

#Creating aggregated weather features, setting temporal scale to annual and cleaning data

# print(weather_df.columns)
# print(weatherStations_df.columns)
# weather_df = weather_df.rename(columns={'stn':'usaf'})
# weatherMerged_df = weatherStations_df.merge(weather_df, on=['usaf', 'wban'])
# weatherMerged_df.to_parquet("/Users/db/Desktop/pollution-weather-exploration/Data/weatherMerged.parquet")
# print(len(weather_df))
# print(len(weatherMerged_df))
weatherMerged_df = pd.read_parquet("/Users/db/Desktop/pollution-weather-exploration/Data/weatherMerged.parquet")
weatherMerged_df['County'] = weatherMerged_df['County'].apply(cleanCountyName)
print(weatherMerged_df.columns)
ID_columns = ['usaf', 'wban']
geoColumns = ['lat', 'lon', 'County']
numericColumns = ['temp', 'count_temp', 'dewp', 'count_dewp',
       'slp', 'count_slp', 'stp', 'count_stp', 'visib', 'count_visib', 'wdsp',
       'count_wdsp', 'mxpsd', 'gust', 'max', 'flag_max', 'min', 'flag_min',
       'prcp', 'flag_prcp', 'sndp', 'fog', 'rain_drizzle', 'snow_ice_pellets',
       'hail', 'thunder', 'tornado_funnel_cloud']
weatherMerged_df[numericColumns] = weatherMerged_df[numericColumns].apply(pd.to_numeric, errors='coerce')
aggregateDictionary = {}
for column in numericColumns:
    aggregateDictionary[column] = ['min', 'max', 'mean', 'median']
weatherMergedAgg_df = weatherMerged_df.groupby(ID_columns, as_index=False)[numericColumns].agg(aggregateDictionary)
weatherMergedAgg_df.columns = ['_'.join(col).strip('_') for col in weatherMergedAgg_df.columns.values]
uniqueStations = weatherMerged_df[['usaf', 'wban','lat', 'lon', 'County', 'state']].drop_duplicates()
weatherMergedAgg_df = weatherMergedAgg_df.merge(uniqueStations, on=['usaf', 'wban'], how='left')
weatherMergedAgg_df = weatherMergedAgg_df.rename(columns={'state':'State_Abbreviations'})
print(len(weatherMergedAgg_df ))
print(weatherMergedAgg_df.head())
print(weatherMergedAgg_df.columns)


#Cleaning pollution data

pollution_df['County'] = pollution_df['County'].apply(cleanCountyName)
pollution_df['State_Abbreviations'] = pollution_df['State'].map(stateFullToAbbreviation)
print(pollution_df.columns)
print(pollution_df.head(6))

def selectBestPollutionMeasurement(df):    
    measurementPriority = {
        # Ozone: 8-hour standard is current EPA primary standard
        ('Ozone', 'Ozone 8-hour 2015'): 1,
        ('Ozone', 'Ozone 8-Hour 2008'): 2,
        ('Ozone', 'Ozone 8-Hour 1997'): 3,
        ('Ozone', 'Ozone 1-hour 1979'): 4,  
        # PM2.5: Annual and 24-hour standards
        ('PM25 - Local Conditions', 'PM25 Annual 2012'): 1,
        ('PM25 - Local Conditions', 'PM25 24-hour 2006'): 2,
        ('PM25 - Local Conditions', 'PM25 24-hour 1997'): 3,
        # CO: 8-hour and 1-hour standards  
        ('Carbon monoxide', 'CO 8-hour 1971'): 1,
        ('Carbon monoxide', 'CO 1-hour 1971'): 2,
    }    
    df['measurement_key'] = list(zip(df['Parameter'], df['pollutant_standard']))
    df['priority'] = df['measurement_key'].map(measurementPriority).fillna(999)
    
    # For each county and parameter, select the highest priority measurement
    bestMeasurements = df.loc[df.groupby(['County', 'State_Abbreviations', 'Parameter'])['priority'].idxmin()]
    
    # If multiple sites have the same priority measurement, average across sites
    finalAgg = bestMeasurements.groupby(['County', 'State_Abbreviations', 'Parameter']).agg({
        'arithmetic_mean': 'mean',  
        'first_max_value': 'mean',
        'ninetieth_percentile': 'mean',
        'latitude': 'first',        
        'longitude': 'first',
        'pollutant_standard': 'first',  
        'units_of_measure': 'first'
    }).reset_index()
    
    return finalAgg

def createCleanPollutionWide(df):
    # Get best measurements
    bestPollution = selectBestPollutionMeasurement(df)
    
    coords_df = bestPollution.groupby(['County', 'State_Abbreviations']).agg({
    'latitude': 'first',
    'longitude': 'first',
    }).reset_index()
    
    # Pivot to wide format
    pollutionWide = bestPollution.pivot_table(
        index=['County', 'State_Abbreviations'],
        columns='Parameter',
        values=['arithmetic_mean', 'first_max_value'],
        aggfunc='first'
    ).reset_index()
    
    pollutionWide.columns = ['_'.join(col).strip('_') if col[1] else col[0] 
                          for col in pollutionWide.columns]
    
    pollutionWide = pollutionWide.merge(coords_df, on=['County', 'State_Abbreviations'], how='left')  
    # Rename for clarity
    columnRenames = {}
    for col in pollutionWide.columns:
        if 'arithmetic_mean_' in col:
            param = col.replace('arithmetic_mean_', '').replace(' ', '_').replace('-', '_')
            columnRenames[col] = f'{param}_mean'
        elif 'first_max_value_' in col:
            param = col.replace('first_max_value_', '').replace(' ', '_').replace('-', '_')
            columnRenames[col] = f'{param}_max'
        elif 'distanceFromSite_' in col:
            param = col.replace('distanceFromSite_', '').replace(' ', '_').replace('-', '_')
            columnRenames[col] = f'{param}_distance'
    
    pollutionWide = pollutionWide.rename(columns=columnRenames)
    return pollutionWide

pollutionClean_df = createCleanPollutionWide(pollution_df)

print(f"Clean pollution dataset shape: {pollutionClean_df.shape}")
print(f"Columns: {list(pollutionClean_df.columns)}")
print(f"Sample of data:")
print(pollutionClean_df.head())

#Cleaning and labelling census data with appropriate county based on FIPS codes

census_df['geo_id'] = census_df['geo_id'].apply(cleanGeoID)
census_df = census_df.rename(columns={'geo_id':'fips_code'})
fips_df['fips_code'] = fips_df['fips_code'].astype(str).str.zfill(5)
censusLabelled_df = census_df.merge(fips_df, on='fips_code')
censusLabelled_df = censusLabelled_df.rename(columns={'county_name':'County', 'state_abbr':'State_Abbreviations'})
censusLabelled_df['County'] = censusLabelled_df['County'].apply(cleanCountyName)
print(len(census_df))
print(len(censusLabelled_df))
print(censusLabelled_df['County'].head())
print(censusLabelled_df.columns)

#Checking for counties expressed in census data that are not represented in both pollution and weather data

countiesInWeatherData = weatherMergedAgg_df[['County','State_Abbreviations']].drop_duplicates().values.tolist()
print(len(countiesInWeatherData))
countiesInPollutionData = pollutionClean_df[['County', 'State_Abbreviations']].drop_duplicates().values.tolist()
print(len(countiesInPollutionData))
countiesInCensusData = censusLabelled_df[['County','State_Abbreviations']].drop_duplicates().values.tolist()
print(len(countiesInCensusData))

missingWeatherCounties = []
capturedWeatherCounties = []
missingPollutionCounties = []
capturedPollutionCounties = []

for county in countiesInCensusData:
    if county in countiesInWeatherData:
        capturedWeatherCounties.append(county)
    else:
        missingWeatherCounties.append(county)
    if county in countiesInPollutionData:
        capturedPollutionCounties.append(county)
    else:
        missingPollutionCounties.append(county)
        
print(f"Missing number of counties (weather): {len(missingWeatherCounties)}")
print(f"Captured number of counties (weather): {len(capturedWeatherCounties)}")
print(f"Missing number of counties (pollution): {len(missingPollutionCounties)}")
print(f"Captured number of counties (pollution): {len(capturedPollutionCounties)}")

#Imputing data for counties with missing weather data based on weather stations proximity to county centroids and missing pollution data based on distances from pollution monitoring sites to county centroids

popWeightedCentroids_CBS = 'https://www2.census.gov/geo/docs/reference/cenpop2020/county/CenPop2020_Mean_CO.txt'
allCountyCentroids_df = pd.read_csv(popWeightedCentroids_CBS, dtype='str')
allCountyCentroids_df['COUNAME'] = allCountyCentroids_df['COUNAME'].apply(cleanCountyName)
allCountyCentroids_df = allCountyCentroids_df.rename(columns={'COUNAME':'County', 'STNAME':'State', 'LATITUDE':'lat','LONGITUDE':'lon'})
allCountyCentroids_df['State_Abbreviations'] = allCountyCentroids_df['State'].map(stateFullToAbbreviation)
print(allCountyCentroids_df.head())

weatherMatches = []
pollutionMatches = []

for _, county in allCountyCentroids_df.iterrows():
    countyTuple = [county['County'], county['State_Abbreviations']]
    closestStation = None
    closestSite = None

    if countyTuple in missingWeatherCounties:
        countyCoordinates = (float(county['lat']), float(county['lon']))
        minWeatherDistance = float('inf')
        countyLat = float(county['lat'])
        countyLon = float(county['lon'])
        
        for idx, station in weatherMergedAgg_df.iterrows():
            # Quick bounding box check first (much faster than geodesic)
            latDiff = abs(float(station['lat']) - countyLat)
            lonDiff = abs(float(station['lon']) - countyLon)
            
            # Skip if obviously too far (roughly 1 degree ≈ 111km)
            if latDiff > 0.7 or lonDiff > 0.7:  # ~75km threshold
                continue
            
            stationCoordinates = (float(station['lat']), float(station['lon']))
            distance = geodesic(stationCoordinates, countyCoordinates).kilometers
            
            if distance < minWeatherDistance and distance <=75:
                minWeatherDistance = distance
                closestStation = station.copy()
                print(distance)
                
    if countyTuple in missingPollutionCounties:
        countyCoordinates = (float(county['lat']), float(county['lon']))
        minPollutionDistance = float('inf')
        countyLat = float(county['lat'])
        countyLon = float(county['lon'])
        
        for idx, site in pollutionClean_df.iterrows():
            # Quick bounding box check
            latDiff = abs(float(site['latitude']) - countyLat)
            lonDiff = abs(float(site['longitude']) - countyLon)
            
            if latDiff > 0.7 or lonDiff > 0.7:
                continue
            
            siteCoordinates = (float(site['latitude']), float(site['longitude']))
            pollutionDistance = geodesic(siteCoordinates, countyCoordinates).kilometers
            
            if pollutionDistance < minPollutionDistance and pollutionDistance <=75:
                minPollutionDistance = pollutionDistance
                closestSite = site.copy()
                print(pollutionDistance)

    if closestStation is not None:
        closestStation['name'] = county['County']
        closestStation['County'] = county['County']
        closestStation['lat'] = float(county['lat'])
        closestStation['lon'] = float(county['lon'])
        closestStation['State_Abbreviations'] = county['State_Abbreviations']
        closestStation['distanceFromStation'] = minWeatherDistance
        weatherMatches.append(closestStation)
    
    if closestSite is not None:
        closestSite['name'] = county['County']
        closestSite['County'] = county['County']
        closestSite['latitude'] = float(county['lat'])
        closestSite['longitude'] = float(county['lon'])
        closestSite['State_Abbreviations'] = county['State_Abbreviations']
        closestSite['distanceFromSite'] = minPollutionDistance
        pollutionMatches.append(closestSite)
        
print(len(weatherMatches))
weatherImputed_df = pd.DataFrame(weatherMatches)
pollutionImputed_df = pd.DataFrame(pollutionMatches)
pollution_df['distanceFromSite'] = 0
weatherMergedAgg_df['distanceFromStation'] = 0

#combining imputed data with existing dataset

weatherNotCleanedData_df = pd.concat([weatherMergedAgg_df, weatherImputed_df], ignore_index=True)
pollutionAllData_df = pd.concat([pollutionClean_df, pollutionImputed_df], ignore_index=True)

#cleaning weather dataset after imputation because preserving coordinates was extremely important for filling data in for nearby counties

numeric_agg = weatherNotCleanedData_df.groupby(['County', 'State_Abbreviations']).agg({
    **{col: ['mean', 'median', 'min', 'max', 'std'] 
       for col in weatherNotCleanedData_df.select_dtypes(include=[np.number]).columns 
       if col not in ['distanceFromStation', 'lat', 'lon', 'usaf', 'wban']}
}).reset_index()

metadata_agg = weatherNotCleanedData_df.groupby(['County', 'State_Abbreviations']).agg({
    'distanceFromStation': 'first',
    'lat': 'first', 
    'lon': 'first'
}).reset_index()

numeric_agg.columns = ['_'.join(col).strip('_') if col[1] else col[0] 
                       for col in numeric_agg.columns]

weatherAllData_df = numeric_agg.merge(metadata_agg, on=['County', 'State_Abbreviations'])


#Checking for unique counties in weather and pollution data

secondCheckWeather = weatherAllData_df[['County','State_Abbreviations']].drop_duplicates().values.tolist()
secondCheckPollution = pollutionAllData_df[['County','State_Abbreviations']].drop_duplicates().values.tolist()

print(len(secondCheckWeather))
print(len(secondCheckPollution))

#Printing data to parquet files, combining and reporting

weatherAllData_df.to_parquet("/Users/db/Desktop/pollution-weather-exploration/Data/weatherAllData.parquet")
pollutionAllData_df.to_parquet("/Users/db/Desktop/pollution-weather-exploration/Data/pollutionAllData.parquet")

final_df = censusLabelled_df.merge(
    weatherAllData_df, 
    on=['County', 'State_Abbreviations'], 
    how='left'
).merge(
    pollutionAllData_df, 
    on=['County', 'State_Abbreviations'], 
    how='left'
)

print(f"Final dataset: {len(final_df)} counties")
print(f"With weather data: {final_df['distanceFromStation'].notna().sum()}")
print(f"With pollution data: {final_df['distanceFromSite'].notna().sum()}")
print(f"With both weather and pollution: {(final_df['distanceFromStation'].notna() & final_df['distanceFromSite'].notna()).sum()}")

final_df.to_parquet("/Users/db/Desktop/pollution-weather-exploration/Data/FinalData.parquet")

print("Census data shape:", censusLabelled_df.shape)
print("Weather data shape:", weatherAllData_df.shape) 
print("Pollution data shape:", pollutionAllData_df.shape)

# Check for multiple rows per county

print("\nWeather stations per county (sample):")
print(weatherAllData_df.groupby(['County', 'State_Abbreviations']).size().head(10))

print("\nPollution records per county (sample):")
print(pollutionAllData_df.groupby(['County', 'State_Abbreviations']).size().head(10))





        


    







        








