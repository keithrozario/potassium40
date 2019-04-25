# Potassium40

An application level scanner build on parallel AWS Lambda Functions!

[![Updates](https://pyup.io/repos/github/keithrozario/potassium40/shield.svg)](https://pyup.io/repos/github/keithrozario/potassium40/)  [![Python 3](https://pyup.io/repos/github/keithrozario/potassium40/python-3-shield.svg)](https://pyup.io/repos/github/keithrozario/potassium40/)  [![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/keithrozario/potassium40.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/keithrozario/potassium40/context:python)  ![](https://img.shields.io/github/license/keithrozario/potassium40.svg)

# What is Potassium40

Pottasium40 is application level scanner, that's built for speed..

It uses AWS Lambda functions to invoke up to 1000 parallel functions to perform application level scans on a list of domains/ips.

This project **proof-of-concept** that will grab and download all `robots.txt` files from the cisco umbrella top 1 million. The code can be changed to grab any other file, or check for http-headers, presence of folders etc.

This is **not** a port-scanner. 

It's an application level scanner, that can scan websites for specific content, create a http connection to a specific url, including handling redirects and negotiating TLS connections. If you want to scan ports, I suggest you use masscan.

**NOTE:** Potassium40 runs on Amazon Web Services (AWS), and while for most use-cases it'll fit into the free-tier, you will be charged if you go overboard. A scan of 1 million websites can be run ~10 times before you have to start paying, roughly $0.60 per 1 million websites.


# Running

## Dependencies
Serverless Framework
Python3.6 (or 3.7)

## To Install:

    $ git clone https://github.com/keithrozario/potassium40.git
    $ cd potassium40/lambda
    $ sls deploy
    $ cd ..
    $ python3 -m venv venv/
    $ source venv/bin/activate
    $ pip install -r requirements.txt

For information of what is deployed during the install.

By default all installations are done in `ap-southeast-1` but you can change this. The script uses the serverless framework to deploy, you will need this as well.

## To run :

    $ ./get_robots.py

To scan all 1 million use the following:

    $ ./get_robots.py -n 800 -p 1250 -m 125

the `n` command line argument is the number of lambda invocations. Each lambda will scan 1250 websites, hence 800 would scan all 1 million.

## To uninstall:

    $ cd lambda
    $ sls remove
  
NOTE: an s3 bucket is created on install with the name p40.<uuid> , please delete all files in the bucket before performing sls remove.


# Warning

This tool is still in beta mode. Use at your peril. But look at these results:

![Screenshot](screenshots/prompt_results.png)

# Architecture

I intend to write in more detail about the architecture, but for now here's a quick n' dirty diagram

![Screenshot](screenshots/architecture.png)

# Results

If you just want the robots.txt files, [here's the latest scan I did](https://www.dropbox.com/s/3zja5hfr7zlo9p0/robots.json.gz?dl=0)

# To do

I'll have to clean up this repo. It's too messy :)

# Contact

keith@keithrozario.com
