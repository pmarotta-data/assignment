Assignment:
A customer needs to analyze a dataset of fire incidents in the city of San Francisco. In
order to do so, it requests you to make the data available in a data warehouse and create
a model to run dynamic queries efficiently.

Requirements:
The copy of the dataset in the data warehouse should reflect exactly the current
state of the data at the source.
For the sake of this exercise, assume that the dataset is updated daily at the
source.
The business intelligence team needs to run queries that aggregate these
incidents along the following dimensions: time period, district, battalion.

**Description of the steps and solution provided:**
1- Downloaded dataset along with Data Dictionary from source website provided.
2- Uploaded CSV file to AWS Data lake on S3 bucket.
3- Provisioned a Redshift cluster instance.
4- Created IAM role to allow Redshift to access the S3 bucket.
5- Loaded CSV file from S3 into Redshift using the COPY command.
6- Started to develop the SQL code to explore and understand the raw data, working under RedShift SQL capabilities.
7- Generated DWH model based on the assignment, creating a fact table for the fire incidents and 3 dimension tables for Time, Battalion and District.
8- Validated and commented each step performed.
9- Added 3 example reports to understand how to extract metrics from the model.
