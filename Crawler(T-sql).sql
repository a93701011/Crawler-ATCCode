if OBJECT_ID('ATCClassifyCodes') is null
create table ATCClassifyCodes(
	ATCClassifyCodes varchar(10)
)

if OBJECT_ID('ATCClassifyCodesCrawler') is null
create table ATCClassifyCodesCrawler(
	ATCClassifyCodes varchar(10)
	,ATCClassifyName varchar(64)

)

--------------------------------------------------------------------------------
truncate table ATCClassifyCodes
insert into ATCClassifyCodes (ATCClassifyCodes)
select rtrim(ATCClassifyCode) atc from HIS.dbo.PHRMATCClassify

--------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS crawler_atc;
GO
CREATE PROCEDURE crawler_atc 
AS
BEGIN
	EXEC sp_execute_external_script
					@language = N'Python'
				  , @script = N'from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
from time import sleep

IDIR = "./output/"
Input = "./"

#df = pd.read_csv( Input + "atc_code.csv", names=["ATCClassifyCodes"] )
df = atc_codes
#df[1:5]["ATC"] dicing or silicing


d = dict()
for x in df["ATCClassifyCodes"].to_dict().values():
     url= "https://www.whocc.no/atc_ddd_index/?code="+x
     try:
         res = requests.post(url)
     except ConnectionError:
        sleep(1)
        res = requests.post(url)
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
'
	, @input_data_1 = N'select top 10 ATCClassifyCodes from ATCClassifyCodes'
	, @input_data_1_name = N'atc_codes'
	--, @params = N'@py_model varbinary(max)'
	--, @py_model = @py_model
	with result sets (("ATCClassifyCodes" varchar(10), "ATCClassifyName" varchar(64)));

END;
GO

--------------------------------------------------------------------------------
truncate table ATCClassifyCodesCrawler

INSERT INTO ATCClassifyCodesCrawler
EXEC crawler_atc;