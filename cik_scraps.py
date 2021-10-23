#%%
import pandas as pd
import sqlite3

#%%
# Read sqlite query results into a pandas DataFrame
conn = sqlite3.connect("fortune500.sqlite")
df = pd.read_sql_query("SELECT * from publiccompanies", conn)

# %%
# Read my ticker to CIK code mapping
ciks = pd.read_csv("ticker.txt", header=None, delimiter="\t")
ciks.columns = ["ticker", "cik"]
ciks["cik"] = [str(cik).zfill(10) for cik in ciks.cik]
ciks["ticker"] = ciks["ticker"].str.upper()

# %%
# Join my CIK codes to the Fortune 500 DataFrame
# Testing the waters first
df_test = df.merge(ciks, how="left", on="ticker", validate="1:1", indicator=True)
df_test[df_test["cik"].isnull()][
    ["ranking", "name", "ticker", "cik"]
]  # check for missing cik

# %%
# Correcting wrong tickers
correct_tickers = {
    "ABDE": "ADBE",
    "BRKA": "BRK-A",
    "RXN": "RRX",
    "MXIM": "MXIM",  # cik 743316
    "MOGA": "MOGA",  # cik 743316
    "RBC": "RRX",
    "VAR": "VAR",  # cik 203527
    "BFB": "BF-B",
    "WINMQ": "WINMQ",  # cik 1282266
    "HTZGQ": "HTZZ",
    "MIK": "MIK",  # cik 1593936
}

df["ticker"].replace(
    to_replace=correct_tickers.keys(), value=correct_tickers.values(), inplace=True
)
#%%
# Actually joining the dfs
df = df.merge(ciks, how="left", on="ticker", indicator=True)

#%%
df[df["cik"].isnull()][["ranking", "name", "ticker", "cik"]]  # check for missing cik

#%%
# manually adding cik codes for those missing
manual_ciks = {
    "MXIM": "0000743316",
    "MOGA": "0000067887",
    "VAR": "0000203527",
    "WINMQ": "0001282266",
    "MIK": "0001593936",
}  

# %%
df.loc[df["ticker"].isin(manual_ciks.keys()), "cik"] = manual_ciks.values()

# At this point I have a clean DF with all the CIK codes

# %%
# Update db
conn = sqlite3.connect("fortune500.sqlite")
df.to_sql(name="publiccompanies", con=conn, index=True, if_exists="replace")








# print(df[df["ranking"]==928]["name"])
# print([df[["cik","name"]].sample()])
# df[df["ticker"]=="AAPL"]

# # %%
# response = requests.get(
#     "https://data.sec.gov/api/xbrl/companyfacts/CIK0001468174.json",
#     headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",},
# )
# response.json()
# # %%

