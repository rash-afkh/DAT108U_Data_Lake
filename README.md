# DAT108U_Data_Lake (Sparkify Data Lake ETL)
Udacity data lake with Apache Spark


This project demonstrates an ETL (Extract, Transform, Load) pipeline using Apache Spark for processing song and log data, and then storing the results in a data lake on Amazon S3.

## Project Structure

- `dl.cfg`: Configuration file containing AWS access keys.
- `etl.py`: Python script for ETL processing using Spark.

## Prerequisites

Before running the ETL pipeline, make sure to set up your AWS access keys in `dl.cfg`.

## Getting Started

To get started, follow these steps:

1. Clone this repository to your local machine.

2. Set up your AWS access keys in the `dl.cfg` file:

   ```ini
   [IAM]
   AWS_ACCESS_KEY_ID = Your_AWS_Access_Key
   AWS_SECRET_ACCESS_KEY = Your_AWS_Secret_Key
   ```

3. Install the required dependencies, including Apache Spark.

4. Run the ETL pipeline by executing the `etl.py` script:

   ```bash
   python etl.py
   ```

   This will process song and log data, creating tables and storing the results in a specified S3 bucket.

## ETL Process

The ETL process is divided into two main functions:

- `process_song_data`: Processes song data and creates tables for songs and artists.
- `process_log_data`: Processes log data and creates tables for users, time, and songplays.

## Project Dependencies

This project depends on the following libraries:

- PySpark
- configparser
- datetime

You can install these dependencies using `pip`.


## Acknowledgments

- [Udacity](https://www.udacity.com) for providing the project template and data.

## Contact

For any questions or feedback, feel free to contact the project owner:
- Rash Afkhami
- Email: rash.afkhami@gmail.com
- GitHub: rash-afkh
