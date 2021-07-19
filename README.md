##Overview

In **load** mode, the script is made to download events.csv file, clean it and store data in an SQL db.
 
 In **compute** mode, the plan is to make queries on the db to retrieve data expected by the user

##Installation

> pip install requirements.txt

##Usage

Store the 04/2021 events.csv file in database
> python main.py --mode load --year 2021 --month 4

Compute data based on some parameters (not implemented yet)
> python main.py --mode compute --config cfg.conf
