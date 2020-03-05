#!/usr/bin/env python3

import pandas as pd
import git
import os

def recordMatcher(frameslist, columnName):
  """
  Takes multiple pandas dataframes and aligns the records using one column as a key

  Parameters
  ----------
  frameslist : list
               list of pandas dataframes to merge to a consistent format
  columnName : string name of column to use as a key for aligning records

  Returns
  ----------

  """

def sumMatching(dataFrame, matchingColumnName, sumColumnNames, carryoverColumns):
  """
  Condenses pandas dataframes over duplicate entries in a given column

  Parameters
  ----------
  dataFrame 		: pandas dataframe
               		  target frame upon which to perform this operation
  matchingColumnName	: string
                	  name of column to use as a key for this sum
  sumColumnNames	: list
        		  list of strings to sum over
  carryoverColumns	: list
			  list of strings to carryover intact

  Returns
  ----------
  returnFrame		: pandas dataframe
                  	  frame summed along duplicate entries in a given column
  """

  # Input verification
  try:
    dataFrame[matchingColumnName]
  except:
    print("ERROR: sumMatching columnName not found in provided dataFrame, or dataframe corrupt")
    return dataFrame

  # Records indexing
  nrecords	= len(dataFrame)
  inputrecords	= 0

  # Creating returnFrame columns
  cleanmatching = matchingColumnName.replace("/", "or")
  returnColumns = (sumColumnNames + [cleanmatching] + carryoverColumns).copy()
  # Kinetica Cleanup
  for i, name in enumerate(returnColumns):
    returnColumns[i] = name.replace("/", "or")
    returnColumns[i] = name.replace(" ", "")
  # Creating returnFrame
  returnFrame   = pd.DataFrame(columns=returnColumns)

  # For every entry in the key column
  for record in dataFrame[matchingColumnName]:
    # See if we have an entry for that in returnFrame yet
    if record not in returnFrame[cleanmatching].to_list():
      # If not, sum data with matching entries
      regionSum = dataFrame.loc[dataFrame[matchingColumnName] == record, sumColumnNames].sum()
      # Add carryoverColumns
      carryover = []
      for column in carryoverColumns:
        carryover.append(dataFrame.loc[dataFrame[matchingColumnName] == record, column].to_list()[0])
      # Add a new entry to returnFrame
      returnFrame.loc[inputrecords] = regionSum.to_list() + [record] + carryover
      # increment the number of records inputted
      inputrecords += 1

  return returnFrame

def addCoordinates(dataFrame, dataMatchColumn, coorFrame, coorMatchColumn):
  """

  """

  # Input verification
  try:
    dataFrame[dataMatchColumn]
  except:
    print("ERROR: addCoordinates columnName not found in provided dataFrame, or dataframe corrupt")
    return dataFrame
  try:
    coorFrame[coorMatchColumn]
  except:
    print("ERROR: addCoordinates columnName not found in provided coordinates Frame, or dataframe corrupt")
    return dataFrame


