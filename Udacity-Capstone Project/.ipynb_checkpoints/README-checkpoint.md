# Temperature and US Immigration Data ETL Pipeline
### Data Engineering Capstone Project

#### Project Summary
I94 Immigration data and city temperature data will be used to create a database that is optimized to query and analyze immigration events. An ETL pipeline is to be build with these to data sources to create the database. Finally, the database will be used to access immigration behaviour to location temperatures.

See short descriptions of the data below:

* **data/18-83510-I94-Data-2016/**: US I94 immigration data from 2016 (Jan-Dec).

  * Source: US National Tourism and Trade Office https://travel.trade.gov/research/reports/i94/historical/2016.html
  * Description: I94_SAS_Labels_Descriptions.txt file contains descriptions for the I94 data.
    * I94 dataset has SAS7BDAT file per each month of the year (e.g. i94_jan16_sub.sas7bdat).
    * Each file contains about 3M rows
    * Data has 28 columns containing information about event date, arriving person, airport, airline, etc.

  * **data/i94_airport_codes.xlsx**: Airport codes and related cities defined in I94 data description file.

    * Source: https://travel.trade.gov/research/reports/i94/historical/2016.html
    * Description: I94 Airport codes data contains information about different airports around the world.
      * Columns: i94port, i94_airport_name
      * Data has 660 rows and 2 columns.

  * **data/i94_country_codes.xlsx**: Country codes defined in US I94 Immigration data description file.

    * Source: https://travel.trade.gov/research/reports/i94/historical/2016.html
    * Description: I94 Country codes data contains information about countries people come to US from.
      * Columns: i94cit, i94_country_code
      * Data has 289 rows and 2 columns.

  * **data/airport-codes.csv**: Airport codes and related cities.

    * Source: https://datahub.io/core/airport-codes#data
    * Description: Airpot codes data contains information about different airports around the world.
      * Columns: Airport code, name, type, location, etc.
      * Data has 48304 rows and 12 columns.

  * **data/iso-3166-country-codes.json**: World country codes (ISO-3166)

    * Source: https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes
      ISO-3166-1 and ISO-3166-2 Country and Dependent Territories Lists with UN Regional Codes
    * ISO-3166: https://www.iso.org/iso-3166-country-codes.html

## Database

US I94 Immigrants Insights database schema has a star design. Start design means that it has one Fact Table having business data, and supporting Dimension Tables. Star DB design is maybe the most common schema used in ETL pipelines since it separates Dimension data into their own tables in a clean way and collects business critical data into the Fact table allowing flexible queries.
The Fact Table can be used to answer for example the following question: How much different nationalities came to US through which airport.

#### Data dictionary 

**Fact Table** - I94 immigration data joined with the city temperature data on i94port
Columns:
   - i94yr = 4 digit year,
   - i94mon = numeric month,
   - i94cit = 3 digit code of origin city,
   - i94port = 3 character code of destination USA city,
   - arrdate = arrival date in the USA,
   - i94mode = 1 digit travel code,
   - depdate = departure date from the USA,
   - i94visa = reason for immigration,
   - AverageTemperature = average temperature of destination city,

**Dimension Table** - I94 immigration data Events
Columns:
   - i94yr = 4 digit year
   - i94mon = numeric month
   - i94cit = 3 digit code of origin city
   - i94port = 3 character code of destination USA city
   - arrdate = arrival date in the USA
   - i94mode = 1 digit travel code
   - depdate = departure date from the USA
   - i94visa = reason for immigration

**Dimension Table** - temperature data
Columns:
- i94port = 3 character code of destination city (mapped from cleaned up immigration data)
- AverageTemperature = average temperature
- City = city name
- Country = country name
- Latitude= latitude
- Longitude = longitude

#### Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project:
  1. I used Spark since it can easily handle multiple file formats (SAS, csv, etc) that contain large amounts of data. Spark SQL was used to process the input files into dataframes and manipulated via standard SQL join operations to create the tables.
    
* Propose how often the data should be updated and why.
    1. Since the format of the raw files are monthly, we should continue pulling the data monthly.
    
### Scenarios
* Write a description of how you would approach the problem differently under the following scenarios:
    1. the data was increased by 100x.
        - Use Amazon Redshift: It is an analytical database that is optimized for aggregation and read-heavy workloads
    2. The data populates a dashboard that must be updated on a daily basis by 7am every day.
        - Airflow can be used here, create DAG retries or send emails on failures.
        - Have daily quality checks; if fail, send emails to operators and freeze dashboards
    3. The database needed to be accessed by 100+ people.
        - Redshift can help us here since it has auto-scaling capabilities and good read performance