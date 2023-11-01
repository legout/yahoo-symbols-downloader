# Yahoo Symbols Downloader

This is a blazing fast python script that downloads *almost all* yahoo symbols. 

The download result is written into a sqlite database or a [PyDala ParquetDataset](http://github.com/legout/pydala2). The latter can be stored on your local disk or a S3 object storage, like AWS S3, Cloudflare R2, Wasabi Cloud Storage, Backblaze B2 or a self-hosted Minio.

In the first run, all download results are saved. In subsequent runs, only new results are added to the database/dataset.

The Downloader can be run [manually](#1-run-the-script-manually) or can be [scheduled](#2-schedule-the-script-run). 


## Install

### Using pdm
```shell
git clone https://github.com/legout/yahoo-symbols
cd yahoo-symbols
pdm install
```

### Using pip 
**Note** Do not use this, if you want to schedule the download
```shell
pip install git+https://github.com/legout/yahoo-symbols.git
```

## Usage

### 1. Run the script manually.
```shell
python -m yahoo_symbols.run --query-length=2 --types=equity,etf
``` 
or 
```shell
pdm run download --query-length=2 --types=equity,etf
```

### Options

```shell
 --types                                TEXT     [default: equity,etf,mutualfund,currency,future,index,cryptocurrency]
        Asset types to download
 --query-length                         INTEGER  [default: 2]                    
        Length of the queries. Queries are all combinations of letters, numbers and `-` or `.`. 
        e.g. for `query-length=2``, theses are, "a", "b",... "aa", "ab", ... 
 --batch-size                           INTEGER  [default: 1000]                    
        The number of queries to run in each batch
 --storage-path                         TEXT     [default: yahoo-symbols]
        The path to store the results 
 --storage-type                         TEXT     [default: s3]
        The type of storage to use. Valid options are: `s3`, ´local´ or ´sqlite´
 --s3-profile                           TEXT     [default: default]
        The S3 profile to use if storage_type is "s3". 
        It is neccessary to define your profile in ~/.aws/credentials
 --s3-bucket                            TEXT     [default: None]   
        The S3 bucket to use
 --random-proxy         --no-random-proxy       [default: no-random-proxy]         
        Whether to use a random proxy. Only recommended, if you configured your webshare.io account (see below) 
 --random-user-agent    --no-random-user-agent  [default: no-random-user-agent]    
        Whether to use a random user agent
 --concurrency                           INTEGER  [default: 25] 
        Number of concurrent downloads.
 --max-retries                           INTEGER  [default: None]                    
        Number of max retries.               
```

### 2. Schedule the script run

#### 1. Edit the settings
In the file `config/settings.yml` you can configure settings like the cron string, storage type, asset types to download,...
```yaml
parameters:
  download:
    concurrency: 25
    max_retries: 5
    random_delay_multiplier: 5
    random_proxy: true
    random_user_agent: true
    verbose: false
  run:
    batch_size: 1000
    query_length: 2
    types:
      - equity
      - etf
      - mutualfund
      - cryptocurrency
      - index
      - currency
      - future
schedule:
  cron: 30 23 * * 1,3,5
  logging:
    filename: tasks
    path: null
    repo: csv
storage:
  local:
    bucket: yahoo-symbols
    partitioning: hive
    path: symbol-info
  s3:
    bucket: yahoo-symbols
    partitioning: hive
    path: symbol-info
    profile: minio
  sqlite:
    path: yahoo-symbols.db
  type: s3
```
#### 2. (Optional) Configure WebShare.io proxies
see [below](#configure-webshareio-proxies)

#### 3. Start the scheduler:
```shell
pdm run start_scheduler
```

## Resulting Database/Dataset


|    | symbol   | exchange   | first_trade_date    |   gmt_off_set_milliseconds | long_name                                     | short_name                      | timezone         | underlying_symbol   | currency   | market    |   exchange_data_delay | exchange_name   | address1                         | address2                    | city       | country        | fax           |   full_time_employees | industry                            | industry_disp                  | industry_key                   | phone           | sector             | sector_disp        | sector_key         | state   | website                                                             | zip      | added               | type   |
|---:|:---------|:-----------|:--------------------|---------------------------:|:----------------------------------------------|:--------------------------------|:-----------------|:--------------------|:-----------|:----------|----------------------:|:----------------|:---------------------------------|:----------------------------|:-----------|:---------------|:--------------|----------------------:|:------------------------------------|:-------------------------------|:-------------------------------|:----------------|:-------------------|:-------------------|:-------------------|:--------|:--------------------------------------------------------------------|:---------|:--------------------|:-------|
|  0 | AALB.AS  | AMS        | 1998-07-20 07:00:00 |                    3600000 | Aalberts N.V.                                 | AALBERTS N.V.                   | Europe/Amsterdam | AALB.AS             | EUR        | nl_market |                     0 | Amsterdam       | Word Trade Center Utrecht        | Stadsplateau 18 PO Box 1218 | Utrecht    | Netherlands    |               |                 14597 | Specialty Industrial Machinery      | Specialty Industrial Machinery | specialty-industrial-machinery | 31 30 307 9301  | Industrials        | Industrials        | industrials        |         | https://www.aalberts.com                                            | 3521 AZ  | 2023-10-29 00:00:00 | equity |
|  1 | ABN.AS   | AMS        | 2015-11-20 08:00:00 |                    3600000 | ABN AMRO Bank N.V.                            | ABN AMRO BANK N.V.              | Europe/Amsterdam | ABN.AS              | EUR        | nl_market |                     0 | Amsterdam       | Gustav Mahlerlaan 10             |                             | Amsterdam  | Netherlands    |               |                 20153 | Banks—Diversified                   |                                |                                | 31 20 343 4343  | Financial Services |                    |                    |         | https://www.abnamro.nl                                              | 1082 PP  | 2023-10-29 00:00:00 | equity |
|  2 | ACOMO.AS | AMS        | 2000-01-03 08:00:00 |                    3600000 | Acomo N.V.                                    | ACOMO N.V.                      | Europe/Amsterdam | ACOMO.AS            | EUR        | nl_market |                     0 | Amsterdam       | WTC, Beursplein 37               | 21st floor                  | Rotterdam  | Netherlands    |               |                  1109 | Food Distribution                   | Food Distribution              | food-distribution              | 31 10 405 1195  | Consumer Defensive | Consumer Defensive | consumer-defensive |         | https://www.acomo.nl                                                | 3011 AA  | 2023-10-29 00:00:00 | equity |
|  3 | AXS.AS   | AMS        | 2007-09-18 07:00:00 |                    3600000 | Accsys Technologies PLC                       | ACCSYS TECHNOLOGIES PLC         | Europe/Amsterdam | AXS.AS              | EUR        | nl_market |                     0 | Amsterdam       | 3 Moorgate Place                 | 4th Floor                   | London     | United Kingdom |               |                   245 | Lumber & Wood Production            |                                |                                | 44 20 7421 4300 | Basic Materials    |                    |                    |         | https://www.accsysplc.com                                           | EC2R 6EA | 2023-10-29 00:00:00 | equity |
|  4 | CTCA1.AS | AMS        | 2021-06-30 07:00:00 |                    3600000 | Climate Transition Capital Acquisition I B.V. | CLIMATE TRANSITION CAPITAL ACQU | Europe/Amsterdam | CTCA1.AS            | EUR        | nl_market |                     0 | Amsterdam       | Basisweg 10                      |                             | Amsterdam  | Netherlands    |               |                   nan | Shell Companies                     |                                |                                | 31 20 262 0230  | Financial Services |                    |                    |         | https://climatetransitioncapital.com                                | 1043 AP  | 2023-10-29 00:00:00 | equity |
|  5 | HYDRA.AS | AMS        | 2008-10-24 07:00:00 |                    3600000 | Hydratec Industries NV                        | HYDRATEC Gewone aandelen        | Europe/Amsterdam | HYDRA.AS            | EUR        | nl_market |                     0 | Amsterdam       | Spoetnik 20                      |                             | Amersfoort | Netherlands    |               |                  1032 | Farm & Heavy Construction Machinery |                                |                                | 31 33 469 7325  | Industrials        |                    |                    |         | https://www.hydratec.nl                                             | 3824 MG  | 2023-10-29 00:00:00 | equity |
|  6 | INBRA.AS | AMS        | 2022-03-14 08:00:00 |                    3600000 | Intereffekt Active Leverage Brazil            | INTEREFF ACTIVE LEV BRAZIL      | Europe/Amsterdam | INBRA.AS            | EUR        | nl_market |                     0 | Amsterdam       | Sewei 2                          |                             | Joure      | Netherlands    |               |                   nan |                                     |                                |                                |                 |                    |                    |                    |         | https://www.intereffektfunds.nl/beleggen-brazilie/fondsprofiel.html |          | 2023-10-29 00:00:00 | equity |
|  7 | WHA.AS   | AMS        | 2006-10-10 07:00:00 |                    3600000 | Wereldhave N.V.                               | WERELDHAVE Gewone aandelen      | Europe/Amsterdam | WHA.AS              | EUR        | nl_market |                     0 | Amsterdam       | WTC Schiphol, Tower A, 3rd floor | Schiphol Boulevard 233      | Schiphol   | Netherlands    | 31 20 7027801 |                    93 | REIT - Retail                       | REIT - Retail                  | reit-retail                    | 31 20 7027800   | Real Estate        | Real Estate        | real-estate        |         | https://www.wereldhave.com                                          | 1118 BH  | 2023-10-29 00:00:00 | equity |
|  8 | ABDP.AQ  | AQS        | 2014-10-14 07:00:00 |                          0 | AB Dynamics PLC                               | AB DYNAMICS PLC                 | Europe/London    | ABDP.AQ             | GBp        | gb_market |                     0 | Aquis AQSE      |                                  |                             |            |                |               |                   nan |                                     |                                |                                |                 |                    |                    |                    |         |                                                                     |          | 2023-10-29 00:00:00 | equity |
|  9 | AXS.AQ   | AQS        | 2014-10-14 07:00:00 |                          0 | Accsys Technologies PLC                       | ACCSYS TECHNOLOGIE              | Europe/London    | AXS.AQ              | GBp        | gb_market |                     0 | Aquis AQSE      |                                  |                             |            |                |               |                   nan |                                     |                                |                                |                 |                    |                    |                    |         |                                                                     |          | 2023-10-29 00:00:00 | equity |






<hr>

## Tips
### Query Length

The number of requests to the yahoo symbol search api endpoint (and therefore, the number of found symbols) increases with the given query length.
The benchmarks of this script for one asset type are (tested for type `equity`):

| --query-length               | 1    | 2     | 3      | 4         |
| ---------------------------- | ---- | ----- | ------ | --------- |
| number of requests           | 38   | 1.482 | 56.354 | 2.141.490 |
| estimated download duration* | ~ 3s | ~1min | ~10min | ~3h       |

**Note** `--query-length=2` already finds almost all available symbols.


## Use of a random proxy server.

**Note** This script should work fine without using random proxies.

When using the  option `--random-proxy`  free proxies* are used. Usage of free proxies is not recommended, as these are not reliable. But maybe you are lucky. Instead use a proxy service like [webshare.io](https://www.webshare.io/?referral_code=upb7xtsy39kl)

### Configure Webshare.io proxies
I am using proxies from [webshare.io](https://www.webshare.io/?referral_code=upb7xtsy39kl). I am very happy with their service and the pricing. If you wanna use their service too, sign up (use the [this link](https://www.webshare.io/?referral_code=upb7xtsy39kl) if you wanna support my work) and choose a plan that fits your needs. In the next step, go to Dashboard -> Proxy -> List -> Download and copy the download link. Set this download link as an environment variable `WEBSHARE_PROXIES_URL`  before running the download script. 

*Export WEBSHARE_PROXIES_URL in your linux shell*
```shell
$ export WEBSHARE_PROXIES_URL="https://proxy.webshare.io/api/v2/proxy/list/download/abcdefg1234567/-/any/username/direct/-/"
```

You can also set this environment variable permanently in an `.env` file in your home folder or current folder or in your command line config file (e.g. `~/.bashrc`).

*Write WEBSHARE_PROXIES_URL into .env*
```shell
WEBSHARE_PROXIES_URL="https://proxy.webshare.io/api/v2/proxy/list/download/abcdefg1234567/-/any/username/direct/-/"
```

*or write WEBSHARE_PROXIES_URL into your shell config file (e.g. ~/.bashrc)*
```shell
$ echo 'export WEBSHARE_PROXIES_URL="https://proxy.webshare.io/api/v2/proxy/list/download/abcdefg1234567/-/any/username/direct/-/"' >> ~/.bashrc
```


*Free Proxies are scraped from here:
- "http://www.free-proxy-list.net"
- "https://free-proxy-list.net/anonymous-proxy.html"
- "https://www.us-proxy.org/"
- "https://free-proxy-list.net/uk-proxy.html"
- "https://www.sslproxies.org/"


<hr>

#### Support my work :-)

If you find this useful, you can buy me a coffee. Thanks!

[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/W7W0ACJPB)