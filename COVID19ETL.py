#!/usr/bin/env python3

################################################################################
#
# COVID-19 Coronavirus Dataset Extraction, Transformation, and Loading
#
# Extraction from:
#			Johns Hopkins University
#
# Transformation:
# 			Binning and Averaging
#
# Loading:
#			Kafka
#
################################################################################
#
# Setup and Declarations:

import pandas as pd
import git
import os
import ETL_funcs as ETL
import numpy as np

################################################################################
# Extraction:
# Source: https://github.com/CryptoKass/ncov-data

# Setup and Declarations:

git_dir		   = "/home/foster/Documents/Library/Data/COVID-19/JHU/"
daily_reports	   = "/csse_covid_19_data/csse_covid_19_daily_reports/"
outputdir	   = "/home/foster/Documents/Library/Data/COVID-19/dtiproducts/"
coordfile	   = "/home/foster/Documents/Library/Data/EARTH/country_coordinates.csv"
fipsfile           = pd.read_csv("/home/hayashis/data/fips.csv")
matchingColumnName = "Country/Region"
sumColumnNames     = ["Confirmed", "Deaths", "Recovered"]
carryoverColumns   = ["DataSource", "Last Update"]

# Extract Coordinates File
coordinatesFrame = pd.read_csv(coordfile)

# Set up git repository
g = git.cmd.Git(git_dir)

# Git Pull to keep us up to date
g.pull()

# List the Files
directory_list = os.listdir(git_dir+daily_reports)
directory_list.sort()

# Make a list of the CSVs
csv_list = []
for file in directory_list:
  if "csv" in file:
    csv_list.append(file)

# Import them as Pandas frames
frames_list = []
for csv in csv_list:
  frames_list.append(pd.read_csv(git_dir + daily_reports + csv))

################################################################################
# Transformation:
# I have daily updates, want them in bins of seven days

# Problem:
# These data are sorted for each day by the number infected
# I need to add geographic regions together

# Solution:
# I will create a master list of all geographic regions effected from the most recent frame, since these data are cumulative
# Then I will, for each date range of interest, add the totals from each frame to the master list

# I will also sum over country

regionSummedFrames = []
for i, frame in enumerate(frames_list):
  frame["DataSource"] = csv_list[i][:-4]
  regionSummedFrames.append(ETL.sumMatching(frame, matchingColumnName, sumColumnNames, carryoverColumns))
  
# Standardize Country Names to coordinate-file
# FINDME abstract out and export to a function
for i, frame in enumerate(regionSummedFrames):
  frame["latitude"]  = None
  frame["longitude"] = None
  frame["StillInfected"] = frame["Confirmed"] - frame["Deaths"] - frame["Recovered"]
  frame.loc[frame["CountryorRegion"] == "Mainland China" , "CountryorRegion"] = "China"
  frame.loc[frame["CountryorRegion"] == "North Macedonia", "CountryorRegion"] = "Macedonia"
  frame.loc[frame["CountryorRegion"] == "US"             , "CountryorRegion"] = "United States"
  frame.loc[frame["CountryorRegion"] == "UK"             , "CountryorRegion"] = "United Kingdom"
  frame.loc[frame["CountryorRegion"] == "North Ireland"  , "CountryorRegion"] = "United Kingdom"
  frame.loc[frame["CountryorRegion"] == "Others"         , "CountryorRegion"] = "Diamond Princess"
  frame.loc[frame["CountryorRegion"] == "Ivory Coast"    , "CountryorRegion"] = "Côte d'Ivoire"
  #addCoordinates(frame, "CountryorRegion", coordinatesFrame, "name")
  for record in frame["CountryorRegion"]:
    # Step 1: Add data to records
    # Add lat/lon
    if record.strip() not in coordinatesFrame["name"].to_list():
      if record == "Diamond Princess":
        # Diamond Princess Cruise Ship
        frame.loc[frame["CountryorRegion"] == record, ("latitude", "longitude")] = [35.570100, 139.924233]
      else:
        # None Found
        print("Record "+record+" not found in database of lat/longs")
    else:
      # Extract Lat/Lon
      tempcoords = coordinatesFrame.loc[coordinatesFrame["name"] == record.strip(), ("latitude", "longitude")]
      frame.loc[frame["CountryorRegion"] == record, "latitude"] = tempcoords["latitude"].to_list()
      frame.loc[frame["CountryorRegion"] == record, "longitude"] = tempcoords["longitude"].to_list()
    # Add Log Infected Caluclation
    numInfected = frame.loc[frame["CountryorRegion"] == record, "StillInfected"]
    logInfected = 100*(np.log(float(numInfected.to_list()[0]))+1)
    if logInfected == -np.inf:
      logInfected = 50
    frame.loc[frame["CountryorRegion"] == record,"logInfected"] = logInfected
    # Check if there is any data for one week prior
    if i > 7:
      lastweek = regionSummedFrames[i-7]
      # See if this record exists for last week
      if record in lastweek["CountryorRegion"].to_list():
        # Get values
        CurrentInfected  = frame.loc[frame["CountryorRegion"] == record, "StillInfected"]
        PreviousInfected = lastweek.loc[lastweek["CountryorRegion"] == record, "StillInfected"]
        # See if there were any active infections last week
        if PreviousInfected.to_list()[0] > 0:
          # Calculate percent change
          frame.loc[frame["CountryorRegion"] == record, "PercentChange"] = 100*(CurrentInfected.to_list()[0] - PreviousInfected.to_list()[0]) / PreviousInfected.to_list()[0]
          if CurrentInfected.to_list()[0] > PreviousInfected.to_list()[0]:
            frame.loc[frame["CountryorRegion"] == record, "Positive"] = 1
          else:
            frame.loc[frame["CountryorRegion"] == record, "Positive"] = 0
        # Set to None if any of these aren't true
        else:
          frame.loc[frame["CountryorRegion"] == record, "PercentChange"] = None
          frame.loc[frame["CountryorRegion"] == record, "Positive"] = None
      else:
        frame.loc[frame["CountryorRegion"] == record, "PercentChange"] = None
        frame.loc[frame["CountryorRegion"] == record, "Positive"] = None
    else:
      frame.loc[frame["CountryorRegion"] == record, "PercentChange"] = None
      frame.loc[frame["CountryorRegion"] == record, "Positive"] = None
    # Step 2 copy record to one-csv-per-country
    

# export to csv
finalFrames = regionSummedFrames

for i in range(len(regionSummedFrames)):
  finalFrames[i].to_csv(outputdir+"regionSummed"+csv_list[i], index=False)
