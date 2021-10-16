#%%
import requests
import pandas as pd
import sqlite3
import re
import json

#%%
# To scrape Fortune 500 for full list of companies
MAINSCRAPER = False
if MAINSCRAPER:
    cookies = {
        '_ga': 'GA1.2.488745781.1634319237',
        '_gid': 'GA1.2.1919468814.1634319237',
        '_gcl_au': '1.1.554956561.1634319240',
        'usprivacy': '1---',
        'notice_behavior': 'none',
        'pv': '21',
        '__tbc': '%7Bjzx%7DMKUmQj7fgSDqlMRYpGnQ-CDyTKMMMdOBsCmmy1UTGYZGgHgIsO6eBCQt3VdMOy9xn8PsXyAYn9hj4_1gchHLOSydw0CYcpUIempCW6XQ4_8',
        '__pat': '-18000000',
        '__pvi': '%7B%22id%22%3A%22v-kuta1x5rr4opy8bo%22%2C%22domain%22%3A%22.fortune.com%22%2C%22time%22%3A1634357188987%7D',
        'xbc': '%7Bjzx%7DkxtyMu2hraH3zN_sGn4xBnIlSvzL-8tvbnJ71aU0Fdg_UPiDMXzNBnR0lXdd3vnnbyaw_38nEDvs1eziJy0Wg4Mv0Oj4dXATZVLcSxFGM-xK__SOhJ7CEdO0B9nmSVT5YQCSAhqIy9axi6ZfsIkHNr0w6g__cbm5uC2SGEzqkc8GkX_s3VjCcGzTRH4d-1i28Uo1sJvaxk15ckPldU3aLqWKVRkovIdtszxYIr1WbF5lnV_1jgOJrqPH_1x44XzLAIB1yck0AbC1b6e4mrfVde-a9cqsKBPcmUqsCW5qy-gYpRp4RAUN35D7DfBfNs4Cj-jpgecoNBuyhK2PDrMZUgmts_U4JxLxtuv7_ftwC9_y_YKuBmgZnOff4-n5BKYW9wvS8kN4BdWR1aJiG0dfMbGpQP8f_3SfMG0zWTKmSYhZahUpFVCKftqoswHcQcKalmQMjerZRq1qDSOm5qUNbwmxaPbJGmuqFuuOUzwxOeDcQY8fO6ZvQvmNM3BNe9FQgi6IfWkGUnRy-zO87xwIvQjnnd8JjfV29LTv-Fj0AJIhfuvn1xOfz4mYevCsY-56QlV2gLLpWOw2EQrW2xRwAk36THnS_qLSz80F68-X6R4-ZJrJT6Ocb61_8MTq_NrXdWaKZ-2mrOIpYcsXzEJn_RROhvskKVPIt98muoD2r16WbxDLuYNEHDF5EqN5eyRcCm8ctNfMunLtVwt7CAchYZW5uOxpiwKUGatUwIQs-NDthgD57T6T3aRNINIfhYUMgq_UvjfsuwE4Tom3Tyyeg4FJwIrVcNvofIe7Y8IpX9HiANx1zo5L-9eoAar0IxwO',
        '_pc_accessLevel': 'none',
        '_pc_segment': 'fcasual',
        '_parsely_visitor': '{%22id%22:%22pid=203f7a32f0b69cd6515734152c800bea%22%2C%22session_count%22:5%2C%22last_session_ts%22:1634357156615}',
        'ntv_as_us_privacy': '1---',
        '_ntv_uid': '95d87721-031a-44ff-b12c-2346c23c7606',
        'cX_S': 'kusnhawqd93oiggu',
        '_fbp': 'fb.1.1634319245040.2018298079',
        '__gads': 'ID=432ccdb44bf14037-223cef61a4cc0058:T=1634319244:S=ALNI_Mb0OrbzaCfL1qVUL4hMaEMxiuDqpQ',
        'cX_G': 'cx%3A3odt62vp7ehi23v6vnwf6m2xbl%3A60h1hrp4cj2v',
        '__pil': 'en_US',
        '_gac_UA-142764551-3': '1.1634357151.EAIaIQobChMI2PHN6IbO8wIV0jArCh2v7QC9EAAYASAAEgIn6_D_BwE',
        '_gcl_aw': 'GCL.1634357152.EAIaIQobChMI2PHN6IbO8wIV0jArCh2v7QC9EAAYASAAEgIn6_D_BwE',
        '_gac_UA-97981691-5': '1.1634330385.EAIaIQobChMIqK35iaPN8wIVjZZLBR0nUgoaEAAYASAAEgLTefD_BwE',
        'tpcc_gfortune500': '%7B%22date%22%3A1634330380497%7D',
        'ntvSession': '{}',
        '_parsely_session': '{%22sid%22:5%2C%22surl%22:%22https://fortune.com/fortune500/%22%2C%22sref%22:%22https://www.google.com/%22%2C%22sts%22:1634357156615%2C%22slts%22:1634335200834}',
        '_uetsid': '150840902dde11ec9a643b0826a18087',
        '_uetvid': '15086aa02dde11ec82e4739b2f623639',
        'iris_user_id': 'UP-uOJaalANkmTGILy',
        '_gat_gaTracker': '1',
        '_gat_pianoTracker': '1',
        '_gat_fortuneAnonymized': '1',
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://fortune.com/fortune500/2021/search/',
        'Authorization': 'Basic Zm9ydHVuZTpCcHNyZmtNZCN5SndjWkkhNHFqMndEOTM=',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }

    params = (
        ('list_id', '3040727'),
        ('token', 'Zm9ydHVuZTpCcHNyZmtNZCN5SndjWkkhNHFqMndEOTM='),
    )

    response = requests.get('https://fortune.com/wp-json/irving/v1/data/franchise-search-results', headers=headers, params=params, cookies=cookies)


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
        cookies = {
            "ntvSession": "{}",
            "_ga": "GA1.2.488745781.1634319237",
            "_gid": "GA1.2.1919468814.1634319237",
            "_gcl_au": "1.1.554956561.1634319240",
            "usprivacy": "1---",
            "notice_behavior": "none",
            "pv": "2",
            "__tbc": "%7Bjzx%7DMKUmQj7fgSDqlMRYpGnQ-CDyTKMMMdOBsCmmy1UTGYZGgHgIsO6eBCQt3VdMOy9xn8PsXyAYn9hj4_1gchHLOSydw0CYcpUIempCW6XQ4_8",
            "__pat": "-18000000",
            "__pvi": "%7B%22id%22%3A%22v-kusnh87ndgda1cxs%22%2C%22domain%22%3A%22.fortune.com%22%2C%22time%22%3A1634319260105%7D",
            "xbc": "%7Bjzx%7DnFs_wQ5oyWaXTbDH_b5DUcnsxQKBaBtH77I7RzpN6Jv7WxffzQKh7iXv4oWubNelNZxVaztPreRnD6PveezX9Uu7NWH8fE5mKQMsxfWnPZItBvvlrbKBPJqoe0u_Pp2Jz94ZRLWh6O2fixc3In2E9P0_RVqZ9AAZhhKUwcf5N9C8cA16WSTxNyY5S1JIBCfND8Uo3xBoLeHiIuaJEsr7-j_4D2Z4iKtX8_gf0by6cd2d9vPiCjw5YF0Oe8xRO2lZleDvjpet3rcHgqwiK-hl0xMyjS6kmaGtFe6lBhQGxL6mMAPXGb0F5tYKUzU8x3h44qWwYLnMWggiYBGViiCKC9Fl__WTjubBO5NwDhI1vJeOKnvGp9UmohGjchpy8o_-oc6E5cO7N8EBJeG9TdaRac9tCpri6Wqt8IlOUvwy2lIbcEIXszTqZik8Py3ePUK4ebWxW-IDIh2h3FkijXxKzNEpMySYcaurZtrqlGawwsnmiwt7qW4LXI5ocMuy0P7SmZjBLyEZuO-dB3uUpLTu1NgnWExfgIqGNfJGXZFry9zLtPZGgsshfoR6G3nI3HrKnOPskSgsuJ7elLZtD6CJS67GDVABJ3mqie_K0Fj7kXmVQOk73kVEvLPoksdaIDVOqUV6zTyfRfJ_742mpnvSBZnTruWEhUXNWKCizlYvQXEEVHmeKYezJlVuoZUI3R_VSJF816617coRiR0d8Lk3Ez2P1uFQjcKUUEdcTbEHmZxTHv0L01bXhRW5IibC1XDrMlK9yse2wqyli2tI24Fpnw",
            "_pc_accessLevel": "none",
            "_pc_segment": "fcasual",
            "_parsely_session": "{%22sid%22:1%2C%22surl%22:%22https://fortune.com/company/viatris/fortune500/%22%2C%22sref%22:%22%22%2C%22sts%22:1634319241977%2C%22slts%22:0}",
            "_parsely_visitor": "{%22id%22:%22pid=203f7a32f0b69cd6515734152c800bea%22%2C%22session_count%22:1%2C%22last_session_ts%22:1634319241977}",
            "ntv_as_us_privacy": "1---",
            "_ntv_uid": "95d87721-031a-44ff-b12c-2346c23c7606",
            "cX_S": "kusnhawqd93oiggu",
            "_fbp": "fb.1.1634319245040.2018298079",
            "__gads": "ID=432ccdb44bf14037-223cef61a4cc0058:T=1634319244:S=ALNI_Mb0OrbzaCfL1qVUL4hMaEMxiuDqpQ",
            "cX_G": "cx%3A3odt62vp7ehi23v6vnwf6m2xbl%3A60h1hrp4cj2v",
            "_uetsid": "150840902dde11ec9a643b0826a18087",
            "_uetvid": "15086aa02dde11ec82e4739b2f623639",
            "__pil": "en_US",
            "_gat_pianoTracker": "1",
            "_gat_UA-142764551-6": "1",
            "iris_user_id": "UP-nhWTuPuZTssZpDn",
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }

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

companies

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

#%%
# Creating a database in sqlite
conn = sqlite3.connect("fortune500.sqlite")
company_df.to_sql(name="companies", con=conn, index=True, if_exists="replace")

# %%
