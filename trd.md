##Data computation
- Compute metrics for each period (year, month, day)
- Need flexibility to add new metrics (maintenance script to compute again ? SQL-View principle ?)

##Storage
- DBMS ? SQL / No-SQL ?

##Process infrastructure
1. Run "loading data" script
    - Can be AWS Lambda, faster to implement but warning to limitations (RAM / CPU)
    - Can be on EC2 instance, also fast to implement, but not event based (crontab) and not serverless
    - Can be with AWS ECS (dockerization of process for more flexibility)
2. Data storage : 
    - Keep the file structure, except for the "tags" part => 1 line for each tag ?
    - Remove the "id" column that may be useless 
3. Data anlysis : For each period, we want to store some metrics :
    - If we want fast response to queries, should be better to store metrics (not compute them directly on request) but we loose in flexibility
    - We can have flexibility on metrics analysed if we store only the input data

##Coding strategy
- Develop in python (3.7+)
- Use Pandas lib for processing data
- One script for reading the file and loading data
- One script for reading data + computing metrics (should be on a web server, depends on the processing time required)
- Create a "storage" client with abstraction (in order to change storage transparently)

##Questions
- What period in each file ? (seems the april file contains data from 15/03 to 15/04 ?)
- Delay to access data ? (fast or slow ?) Can accept to wait some minutes / hours after a request ?
