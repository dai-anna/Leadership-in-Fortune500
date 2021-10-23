# Leadership-in-Fortune500
Web-scraping for data on top executives and board members in Fortune500 companies for analysis.

## Scraps of Fortune
The first part of my project involves scraping Fortune.com to obtain some general data including name, ticker symbol, revenue and number of employees of Fortune500 companies and writing the data to a SQLite Database, which we can see a query below:
![Screen Shot 2021-10-15 at 11 31 26 PM](https://user-images.githubusercontent.com/89488845/137572312-f0ad2992-7c6e-444b-961f-2870a90d3fef.png)

### Setup process
1. Install requirements.txt in a virtual environment
```
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

2. Launch the main Fortune site with Inspector open and curl the link with the key data


...

### Sources of data
[Fortune.com](https://fortune.com/fortune500/2021/search/)
[SEC Ticker to CIK](https://www.sec.gov/os/accessing-edgar-data)
