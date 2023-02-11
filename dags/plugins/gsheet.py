# -*- coding: utf-8 -*-
from airflow.models import Variable
from oauth2client.service_account import ServiceAccountCredentials

import base64
import gspread
import json
import logging
import os
import pandas as pd
import pytz


def write_variable_to_local_file(variable_name, local_file_path):
    content = Variable.get(variable_name)
    f = open(local_file_path, "w")
    f.write(content)
    f.close()


def get_gsheet_client():
    data_dir = Variable.get("local_data_dir")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    gs_json_file_path = data_dir + 'google-sheet.json'

    write_variable_to_local_file('google_sheet_access_token', gs_json_file_path)
    credentials = ServiceAccountCredentials.from_json_keyfile_name(gs_json_file_path, scope)
    gc = gspread.authorize(credentials)

    return gc


def p2f(x):
    return float(x.strip('%'))/100


def get_google_sheet_to_csv(
    sheet_uri,
    tab,
    filename,
    header_line=1,
    remove_dollar_comma=0,
    rate_to_float=0):
    """
    Download data in a tab (indicated by "tab") in a spreadsheet ("sheet_uri") as a csv ("filename")
    - if tab is None, then the records in the first tab of the sheet will be downloaded
    - if tab has only one row in the header, then just use the default value which is 1
    - setting remove_dollar_comma to 1 will remove any dollar signs or commas from the values in the CSV file
      - dollar sign might need to be won sign instead here
    - setting rate_to_float to 1 will convert any percentage numeric values to fractional values (50% -> 0.5)
    """

    data, header = get_google_sheet_to_lists(
        sheet_uri,
        tab,
        header_line,
        remove_dollar_comma=remove_dollar_comma)

    if rate_to_float:
        for row in data:
            for i in range(len(row)):
                if str(row[i]).endswith("%"):
                    row[i] = p2f(row[i])

    data = pd.DataFrame(data, columns=header).to_csv(
        filename,
        index=False,
        header=True,
        encoding='utf-8'
    )


def get_google_sheet_to_lists(sheet_uri, tab=None, header_line=1, remove_dollar_comma=0):
    gc = get_gsheet_client()

    # no tab is given, then take the first sheet
    # here tab is the title of a sheet of interest
    if tab is None:
        wks = gc.open_by_url(sheet_uri).sheet1
    else:
        wks = gc.open_by_url(sheet_uri).worksheet(tab)

    # list of lists, first value of each list is column header
    print(wks.get_all_values())
    print(int(header_line)-1)
    data = wks.get_all_values()[header_line-1:]

    # header = wks.get_all_values()[0]
    header = data[0]
    if remove_dollar_comma:
        data = [replace_dollar_comma(l) for l in data if l != header]
    else:
        data = [l for l in data if l != header]
    return data, header


def replace_dollar_comma(lll):
    return [ ll.replace(',', '').replace('$', '') for ll in lll ]
