#%%
import requests
from bs4 import BeautifulSoup


# testing on one company (AAPL) first

url = "https://www.sec.gov/Archives/edgar/data/320193/000032019320000096/aapl-20200926.htm"
headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
response = requests.get(url, headers=headers)

soup = BeautifulSoup(response.text, "lxml")

#%%
tds = soup.find_all(
    "td",
    {
        "style": "border-top:1pt solid #000000;padding:2px 1pt 2px 11.25pt;text-align:left;vertical-align:bottom"
    },
)
for x in tds:
    print(x.text)
# %%
