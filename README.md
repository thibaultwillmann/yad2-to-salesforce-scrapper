# yad2-to-salesforce-scrapper
- scape daily all relevant listings from Yad2 via Excel file into Salesforce CRM and Database
- best deployed in AWS cloud via Lambda, Eventbridge, SQS, S3
## Architecture
### 1. AWS Eventbridge Trigger
- triggers every 24h during early morning hours (no activity on yad2)
- invokes scrape_ids.py
### 2. AWS Lambda scrape_ids.py
- scrapes all yad2_ids of apartments updated in the last 24h in a list of defined cities
- sends the scraped yad2 ids in batches to AWS SQS FIFO queue for further processing.
### 3. AWS SQS Queue
- receives messages from scrape_ids.py as FIFO
- triggers for each message a new instance of AWS Lambda scrapper.py in parallel and passes the message to the Lambda instance
### 4. AWS Lambda scrapper.py
- receives message from AWS SQS Queue containing batch of yad2 ids
- scrapes all relevant data for each id
- saves scraped data as Excel file in AWS S3 bucket
### 5. AWS S3 Bucket
- receives Excel file from AWS Lambda scrapper.py
- triggers for each new Excel file an instance of yad2_excel_convert.py in parallel
### 6. AWS Lambda yad2_excel_convert.py
- receives url to Excel file stored in S3 containing scraped apartment data
- uploads data to Salesforce as Listings, Seller Contacts and Images
- syncronizes the data from Salesforce to Database
## Flow Diagram

## Contact
Do not hesitate to contact me at: thibault.willmann@gmail.com
