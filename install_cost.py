import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import logging
from decorators import timer, debug
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(verbose=True)

KEY = os.getenv("KEY")
url = os.getenv("URL")

# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

streamHandler = logging.StreamHandler()
streamHandler.setFormatter(formatter)
streamHandler.setLevel(logging.WARNING)
logger.addHandler(streamHandler)

fileHandler = logging.FileHandler("log.log")
fileHandler.setFormatter(formatter)
fileHandler.setLevel(logging.DEBUG)
logger.addHandler(fileHandler)

logger.info("Start of program")


class InstallCPS:
    def __init__(self, KEY):
        self.params = {
            "serviceKey": KEY,
            "pnu": None,
            "stdrYear": None,
            "format": "xml",
            "numOfRows": 100000,
            "pageNo": "1",
        }
        self.url = "http://apis.data.go.kr/1611000/nsdi/IndvdLandPriceService/attr/getIndvdLandPriceAttr"

    @timer
    def get_page(self, pnu: str, pageNo: int, year: int = 2021):
        self.params["pageNo"] = str(pageNo)
        self.params["pnu"] = str(pnu)
        self.params["stdrYear"] = year

        response = requests.get(self.url, params=self.params)

        # Parsing the XML response to get the number of rows --> if there exists no data, numRows = 0
        soup = bs(response.text, features="xml")
        numRows = int(soup.find("totalCount").text)

        # Return the result dataframe if there is any record in the given year
        if numRows > 0:
            logger.info(msg=f"{pnu} : There is {numRows} records in {year}")
            return pd.read_xml(response.text, xpath=".//fields/field")

        # Return the result of the right previous year if there is no record in the given year
        return self.get_page(pnu, pageNo, year - 1)


if __name__ == "__main__":
    # Test the class

    load_dotenv()
    installCPS = InstallCPS(KEY)

    bj_dong = pd.read_csv("bj_dong_infos.csv")[["법정동코드", "법정동명"]]

    for idx, code, name in bj_dong.itertuples():
        df = installCPS.get_page(pnu=code, pageNo=1, year=2022)
        df.to_csv(f"output/{name}.csv", encoding="utf-8-sig")
        logger.info(msg=f"{name} - {df.shape}")
