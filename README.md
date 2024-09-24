# Store Monitoring API

## Overview

This project provides a backend service for monitoring restaurant uptime and downtime based on their business hours. Using data from multiple sources, the service generates detailed reports that help restaurant owners track their stores' online statuses during designated operating hours.

## Link for Zipped code on drive with video and database
  -- https://drive.google.com/drive/folders/16OrOWD7FhfGaYMKkQl8JajVR6RqZc5Fo?usp=sharing

## I HAVE ZIPPED THE VIDEO WITH THE CODE AND UPLOADED IT ON DRIVE, CAUSE OF SIZE LIMIT ON GITHUB

## Features

- **Data Ingestion**: The API ingests three types of data from CSV files:
  1. **Store Status Logs**: Records of whether each store is active or inactive at specific timestamps.
  2. **Business Hours**: Defines the operating hours for each store in local time.
  3. **Timezone Data**: Maps each store to its timezone for accurate hour calculations.

- **Report Generation**: The API allows users to generate reports on uptime and downtime for the last hour, day, and week. The report generation includes:
  - Uptime and downtime only within the business hours.
  - Extrapolation of uptime and downtime using interpolation logic based on the status logs.

- **API Endpoints**:
  - **`/trigger_report`**: Triggers report generation and returns a unique `report_id`.
  - **`/get_report/<report_id>`**: Checks the status of the report and allows downloading the generated CSV file once complete.

- ## Required Libraries
  - Flask==2.1.3
  - pandas==1.4.2
  - pytz==2022.1

