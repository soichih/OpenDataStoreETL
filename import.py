#!/usr/bin/env python3

import google_importer

#https://docs.google.com/spreadsheets/d/1ns7jz1dRPXuCqISLZTPG1b6eYC-ds_PuQXLAFkr4UVk/edit?usp=sharing
#TAB1
frame = google_importer.download_spreadsheet(spreadsheetID='1ns7jz1dRPXuCqISLZTPG1b6eYC-ds_PuQXLAFkr4UVk', rangeName='TAB1!A2:E')
frame.to_csv('/tmp/sample.tab1.csv', index=False)

#TODO - ship each records to kafka?
print(frame)

#TAB2
frame = google_importer.download_spreadsheet(spreadsheetID='1ns7jz1dRPXuCqISLZTPG1b6eYC-ds_PuQXLAFkr4UVk', rangeName='TAB2!A2:E')
frame.to_csv('/tmp/sample.tab2.csv', index=False)

#TODO - ship each records to kafka?
print(frame)
