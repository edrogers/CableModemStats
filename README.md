# Cable Modem Stats

A repo of scripts I'm using to monitor the performance of my home cable modem and ISP.

## Setup 

### Put this repo in the right place

First clone this repo. Currently, directory locations are hard-coded (in part because of pipenv).

```
cd /home/ed/
mkdir -p Documents
cd Documents
git clone git@github.com:edrogers/CableModemStats.git
```

### Get the data so far

Sync `modemOutput/modemData.csv` and `speeds/speeds.csv` from S3.

### Create the virtual environment

```
pipenv sync --dev
```

### Add the scripts to the user cron table

```
crontab -l > current_crontab.txt
echo "" >> current_crontab.txt  # Just in case
cat crontab.txt >> current_crontab.txt
crontab -r
crontab -i current_crontab.txt
rm current_crontab.txt
```

## What it does

* Every minute, I scrape the "Signal Stats" page of my cable modem (downstream power, upstream modulation, etc.).
* Every 15 minutes, I run a speedtest.net speedtest.
* Every hour, I 
  * process the signal stats and append the results to a CSV, and
  * process the speedtest results and append those results to a different CSV

