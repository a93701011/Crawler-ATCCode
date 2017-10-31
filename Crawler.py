# -*- coding: utf-8 -*-
"""
Created on Wed Sep 20 18:30:01 2017

@author: a-kaku
"""

from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
from time import sleep

IDIR = "./output/"
Input = "./"

df = pd.read_csv( Input + "atc_code.csv", names=["ATCClassifyCodes"] )
#df = atc_codes
#df[1:5]["ATC"] dicing or silicing



d = dict()
for x in df["ATCClassifyCodes"].to_dict().values():
     url= "https://www.whocc.no/atc_ddd_index/?code="+x
     try:
         res = requests.get(url)
     except ConnectionError:
        sleep(1)
        res = requests.get(url)
     soup = BeautifulSoup(res.text, "html.parser")

     #print(soup.prettify())
     results = soup.find_all("a")
     for result_tag in results:
        hrefstring = result_tag.get("href")
        #print(hrefstring)
        if "code" in hrefstring :
            code = re.search("[A-Z]\d*[A-Z]*\d*",hrefstring)
       
            if code.group() == x :
                d[code.group()] = result_tag.get_text()

sub = pd.DataFrame.from_dict(d, orient="index")
# sub.loc["H03AB"] filter by index

sub.reset_index(inplace=True)
sub.columns = ["ATCCode","ATCName"]
OutputDataSet = sub

sub.to_csv(IDIR+'atc_export.csv', index=False, sep ="|")

    