#%%
import requests
import pandas as pd
import sqlite3
import re
import json
import personal


#%%
# To scrape Fortune 500 for full list of companies
MAINSCRAPER = False
if MAINSCRAPER:
    cookies = personal.main_cookies

    headers = personal.main_headers

    params = personal.main_params

    response = requests.get(
        "https://fortune.com/wp-json/irving/v1/data/franchise-search-results",
        headers=headers,
        params=params,
        cookies=cookies,
    )

    main_site = response.json()

#%%
# To save to disk
if MAINSCRAPER:
    with open("initial_scrape.json", "w") as f:
        json.dump(main_site, f)

#%%
# To load from disk

with open("initial_scrape.json", "r") as f:
    main_site = json.load(f)

#%%
headers_dict = main_site[0]
headers_dict

#%%
data_dict = main_site[1]
data_dict

#%%
data_list = list(data_dict.values())[1]
data_list

#%%
urls_series = pd.DataFrame(data_list)["permalink"]
urls_list = urls_series.tolist()
urls_list

#%%
# To scrape each company's URLs for their ticker symbol; this takes a hot minute do turn off if you don't need it

TICKERSCRAPING = False
if TICKERSCRAPING:
    pattern = r"\"ticker\":\"(?P<ticker>\w{1,7})\""
    tickers_list = []

    for url in urls_list:
        cookies = personal.url_cookies

        headers = personal.url_headers

        response = requests.get(url, headers=headers, cookies=cookies)
        try:
            matches = re.findall(pattern, response.text)[0]
            tickers_list.append({url: matches})
        except:
            matches = "WHOMPWHOMP"

        print(matches + str(len(tickers_list)))

#%%
# To save my trusty scraped tickers to disk

if TICKERSCRAPING:
    result_df = pd.DataFrame(
        {
            "ticker": [list(x.values())[0] for x in tickers_list],
            "url": [list(x.keys())[0] for x in tickers_list],
        }
    )

    result_df.to_csv("results_tickers.csv")

#%%
# To pull my results_tickers.csv back in as a dataframe
result_df = pd.read_csv("results_tickers.csv")

#%%
# To view the remaining nested data in a better format
VIEWINJSON = False
if VIEWINJSON:
    with open("dump.json", "w") as f:
        json.dump(data_list, f)

if VIEWINJSON:
    with open("dump.json", "r") as f:
        hihi = json.load(f)

#%%
# To unnest my data and build a dictionary for each company

companies = []
for company in data_list:
    data = company["fields"]
    company_data = {line["key"]: line["value"] for line in data}
    company_data["permalink"] = company["permalink"]
    companies.append(company_data)

#%%
# To save my cleaned company data as a dataframe
company_df = pd.DataFrame(companies).set_index("title")
company_df = company_df.rename(columns={"rank": "ranking"})

#%%
# To merge my two dfs on URL
company_df = pd.merge(
    company_df,
    result_df,
    how="left",
    left_on="permalink",
    right_on="url",
    validate="1:1",
    indicator=True,
).drop(["permalink", "Unnamed: 0"], axis=1)

#%%
# To clean up company_df column variable type

convert_to_int = [
    "ranking",
    "rankchange1000",
    "f500_employees",
    "rankchange",
    "measure-up-rank",
]
convert_to_float = [
    "f500_revenues",
    "revchange",
    "f500_profits",
    "prftchange",
    "assets",
    "f500_mktval",
]
company_df[convert_to_int] = company_df[convert_to_int].replace("", str(0))
company_df[convert_to_float] = company_df[convert_to_float].replace("", str(0))
company_df[convert_to_int] = company_df[convert_to_int].astype("int")
company_df[convert_to_float] = company_df[convert_to_float].astype("float")
# company_df.loc["revchange", "prftchange"] = company_df.loc["revchange", "prftchange"]/100
company_df.sample(10)
# %%
# To rename companies with no ticker symbol
# name_ticker_dict = {
#     'CHS': 'CHSCO',
#     'Northwestern Mutual':'NWE',
#     'USAA':'',
#     'TIAA':'',
#     'Nationwide':'',
#     'Liberty Mutual Insurance Group':'',
#     'Publix Super Markets':'',
#     'New York Life Insurance':'',
#     'State Farm Insurance':'',
#     'CoreLogic':'',
#     'Teledyne FLIR':'',
#     'Perspecta':'',
#     'Ohio National Mutual':'',
#     'TCF Financial':'',
#     'Federated Mutual Insurance':'',
#     'Knights of Columbus':'',
#     'Country Financial':'',
#     'NLV Financial':'',
#     'Univision Communications':'',
#     'Cooper Tire & Rubber':'',
#     'Amica Mutual Insurance':'',
#     'Hexion':'',
#     'PRA Health Sciences':'',
#     'Puget Energy':'',
#     'Sentry Insurance Group':'',
#     'Medical Mutual of Ohio':'',
#     'Penn Mutual Life Insurance':'',
#     'CUNA Mutual Group':'',
#     'Alexion Pharmaceuticals':'',
#     'Securian Financial Group':'',
#     'FM Global':'',
#     'Graybar Electric':'',
#     'Navistar International':'',
#     'Western & Southern Financial Group':'',
#     'Thrivent Financial for Lutherans':'',
#     'Auto-Owners Insurance':'',
#     'Erie Insurance Group':'',
#     'Pacific Life':'',
#     'Jones Financial (Edward Jones)':'',
#     'Mutual of Omaha Insurance':'',
#     'Farmers Insurance Exchange':'',
#     'Peter Kiewit Sons&#8217;':'',
#     'Core-Mark Holding':'',
#     'Guardian Life Ins. Co. of America':'',
#     'American Family Insurance Group':'',
#     'Land O&#8217;Lakes':'',
#     'Massachusetts Mutual Life Insurance:'',
# }
# %%
# To remove companies with no ticker symbol
company_df_public = company_df[company_df["ticker"].notnull()]
company_df_public = company_df_public.drop(["_merge"], axis=1)# company_df_public = 

#%%
# Creating a database in sqlite
conn = sqlite3.connect("fortune500.sqlite")
company_df_public.to_sql(name="publiccompanies", con=conn, index=True, if_exists="replace")


# %%
