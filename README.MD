# Gloda

Imagine having access to a 1000 core machine with unlimited network capacity. That's what Serverless functions on AWS Lambda.

This project is a proof of concept on spinning up 1000 parallel lambda invocations, to download the `robots.txt` files from all websites on the cisco umbrella 1 million.

General timing to scan all 1 million websites is  

# Framework description

## Functions

All configuration for the serverless functions are in the `deploy/serverless.yml` file. The file identical to that provided by the serverless framework.

Use `lambda/deploy.py` to deploy your functions. By default the script cretes the necessary functions, layers for functions, buckets, and IAM roles for you.

Use `lambda/undeploy.py` to remove your functions, buckets, and IAM roles created by the framework

**Note:** There are two `requirements.txt` files, one for the framework to invoke the functions (found in the root directory), and another for the serverless functions themselves, found in the `lambda/` directory. By keeping two `requirements.txt` files, I keep the serverless functions as light as possible.

# Running

To Install:

`pip install -r requirements.txt`
`cd lambda`
`./deploy.py`

To run (call from the root directory):

`./get_robots.py`

To uninstall:

`lambda/undeploy.py`
