# Yahoo Symbols Downloader

This is a blazing fast python script that downloads *almost all* yahoo symbols. 

The download result is written into a sqlite database or a [PyDala ParquetDataset](http://github.com/legout/pydala2). The latter can be stored on your local disk or a S3 object storage, like AWS S3, Cloudflare R2, Wasabi Cloud Storage, Backblaze B2 or a self-hosted Minio.

In the first run, all download results are saved. In subsequent runs, only new results are added to the database/dataset.

The Downloader can be run [manually](#1-run-the-script-manually) or can be [scheduled](#2-schedule-the-script-run). 


# Quick Start

Use docker and set environemnt variables and run options manually in the command line.

### 1. Run download once
```shell
docker run -it --rm \
       # confiure env variables of needed \
      -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
      -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
      -e AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL} \
      -e WEBSHARE_PROXIES_URL=${WEBSHARE_PROXIES_URL} \
       ghcr.io/legout/yahoo-symbols/yahoo-symbols:latest \
       run \
       --storage-path symbol-info.db \
       --storage-type s3 \
       --s3-bucket yfin-db 
       # see below for more options
```

### 2. Start scheduler
```shell
docker run -it --rm \
       # confiure env variables of needed \
      -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
      -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
      -e AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL} \
      -e WEBSHARE_PROXIES_URL=${WEBSHARE_PROXIES_URL} \
       ghcr.io/legout/yahoo-symbols/yahoo-symbols:latest \
       start-scheduler \
       --storage-path symbol-info.db \
       --storage-type s3 \
       --s3-bucket yfin-db 
       --cron 0 0 * * 1-5 # At 00:00 on every day-of-week from Monday through Friday.
       # see below for more options
```

See below for further [options](#options). 

If you prefer a file-base configuration, create a `.env` file and a `settings.tmol` and set the neccessary environment variables ([s3 configuration](#2-edit-env-example) and [webshare.io proxy configuration](#configure-webshareio-proxies)) in `.env` and edit the options in `settings.toml`.

```shell
docker run -it --rm \
       -v path/to/settings.toml:/app/config/settings.toml \
       --env-file path/to/.env \
       ghcr.io/legout/yahoo-symbols/yahoo-symbols:latest \
       start-scheduler # or run if you want to run the download only once \ 
       --settings /app/config/settings.toml
```

# Install

## Using pdm
```shell
git clone https://github.com/legout/yahoo-symbols
cd yahoo-symbols
pdm install --prod
```

## Using pip 
**Note** Do not use this, if you want to schedule the download
```shell
pip install git+https://github.com/legout/yahoo-symbols.git
```

# Usage

## 1. Run the script manually.

### 1.1. Set options manually
```shell
python -m yahoo_symbols.main run --query-length=2 --types=equity,etf
``` 
or 
```shell
pdm run downloader --query-length=2 --types=equity,etf
```

#### Options

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
 --s3-profile                           TEXT     [default: None]
        The S3 profile to use if storage_type is "s3". 
        When you set a profile here, it is neccessary to define your profile in ~/.aws/credentials. Otherwise, thre s3 credentials are read from the environment variables. Therefore you have to edit the .env-eample file or set them manually.
 --s3-bucket                            TEXT     [default: None]   
        The S3 bucket to use
 --random-proxy         --no-random-proxy       [default: no-random-proxy]         
        Whether or not to use a random proxy. Only recommended, if youhave a webshare.io account and configure it accordingly (see below) 
 --random-user-agent    --no-random-user-agent  [default: no-random-user-agent]    
        Whether or not to use a random user agent
 --concurrency                          INTEGER  [default: 25] 
        Number of concurrent downloads.
 --max-retries                          INTEGER  [default: None]                    
        Number of max retries.            
 --verbose     --no-verbose                      [default: no-verbose]   
        Whether or not to display a progressbar.
 --warnings     --no-warnings                    [default: no-warnings]   
        Wheter or not to display warnings               
 --log-path                             TEXT     [default: None]
        Set this path, if you wanna save the logs.
```

### 1.2. File-based configuration
Optionally, you can set all this options in the file `config/settings.toml` 

```toml
[parameters.download]
concurrency = 10
max_retries = 5
random_delay_multiplier = 10
random_proxy = true
random_user_agent = true
verbose = false
proxies = ""

[parameters.run]
batch_size = 1_000
query_length = 2
types = [
  "equity",
  "etf",
  "mutualfund",
  "cryptocurrency",
  "index",
  "currency",
  "future",
]
log_path = "logs"

[scheduler]
cron = "30 23 * * 1,3,5"

[storage]
type = "s3"

[storage.local]
bucket = "yfin-db"
partitioning = "hive"
path = "symbol-info"

[storage.s3]
bucket = "yfin-db"
partitioning = "hive"
path = "symbol-info"
profile = "swfs"

[storage.sqlite]
path = "yfin-db/symbol-info.db"
```

and run

```shell
python -m yahoo_symbols.main run --settings path/to/config/settings.toml
``` 
or 
```shell
pdm run downloader -settings path/to/config/settings.toml
```

####  Resulting Database/Dataset


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



## 2. Schedule the script run
### 2.1. Configure options manually
```shell
python -m yahoo_symbols.main start_scheduler --query-length=2 --types=equity,etf
``` 
or 
```shell
pdm run scheduler --query-length=2 --types=equity,etf
```

### 2.2. File-based configuration

```shell
python -m yahoo_symbols.main start_scheduler --settings path/to/config/settings.toml
``` 
or 
```shell
pdm run scheduler -settings path/to/config/settings.toml
```



<hr>

# Tips
## Configure S3

When you choose to save the download results in a S3 object storage like AWS, Cloudflare R2, Backblaze or Wasabi Cloud, you have to set the credentials and enpoint_url in `~/.aws/credentials` or set them in the environment variables.

### 1. Edit ~/aws/crendetials
```toml
[profile_name]
aws_access_key_id=my-access-key-id
aws_secret_access_key=my-secret-access-key
# only needed when you do not use AWS S3, e.g. R2
endpoint_url=https://123456.r2.cloudflarestorage.com
```

### 2. Edit .env-example
```
# set your AWS/R2/B2/Wasabi/Storj/... S3 credentials

AWS_ACCESS_KEY_ID=my-access-key-id
AWS_SECRET_ACCESS_KEY=my-secret-access-key
# only needed when you do not use AWS S3, e.g. R2
AWS_ENDPOINT_URL=https://123456.r2.cloudflarestorage.com

...
```
## Query Length

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

You have several options to set a environment variable in Linux or macOS.

#### 1. Set manually
Set it temporarily by running this in your shell.
```shell
$ export WEBSHARE_PROXIES_URL="https://proxy.webshare.io/api/v2/proxy/list/download/abcdefg1234567/-/any/username/direct/-/"
```

You can also set this environment variable permanently in an `.env` file in your home folder or current folder or in your command line config file (e.g. `~/.bashrc`).

#### 2. Write to .env
Write it into an `.env` file and copy this file into your home directory or the directory where you run the download script from.
```shell
echo 'WEBSHARE_PROXIES_URL="https://proxy.webshare.io/api/v2/proxy/list/download/abcdefg1234567/-/any/username/direct/-/"' >> .env
# optional
mv .env ~/.env
```

or use the `.env-example` file in this repository and edit the variable `WEBSHARE_PROXIES_URL`.
```
...
# url to download webshare.io proxy list
WEBSHARE_PROXIES_URL=https://proxy.webshare.io/download/proxy/list
```

#### 3. Write to shell rc file
Write the export command into your shell rc file like `~/.bashrc` or `~/.zshrc`
```shell
$ echo 'export WEBSHARE_PROXIES_URL="https://proxy.webshare.io/api/v2/proxy/list/download/abcdefg1234567/-/any/username/direct/-/"' >> ~/.bashrc
```


**Free Proxies are scraped from here:
- "http://www.free-proxy-list.net"
- "https://free-proxy-list.net/anonymous-proxy.html"
- "https://www.us-proxy.org/"
- "https://free-proxy-list.net/uk-proxy.html"
- "https://www.sslproxies.org/"


<hr>

#### Support my work :-)

If you find this useful, you can buy me a coffee. Thanks!

[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/W7W0ACJPB)