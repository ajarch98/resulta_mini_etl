# resulta_mini_etl
Mini-ETL pipeline for interview task.

## To run:

`$ python3 requests_task.py`

## Summary

Extract from NFL scoreboard and events endpoints.
Transform data into the required formats.
Load data into a json file.

Assumptions, Limitations, and Notes:
 - minimal error-checking required for the API response. In a prod environment, I would have:
   - timeouts and retries on API calls
   - more verbose error handling
   - maybe even APIResponseSerializers
 - response data is not too massive, hence chunking is unrequired.
