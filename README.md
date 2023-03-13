# IOH-Push-Notification

1. Tag Visited Content 

Get all visited content, Query on Impala for myIM3

```
with base as 
    (
        select page, count(distinct msisdn) total_misdn, sum(count(distinct msisdn)) over() as total
        FROM digital.fdn_user_journey_myim3
        where trx_date>=from_timestamp(current_date()- interval 30 days, 'yyyyMMdd') and lower(page) like '%browser%'
        group by 1
        order by 2 desc
    )
    , finale as 
    (
        select *, cast(total_misdn as float)/total as pctg
        from base
        where page not in (select page from digital.PN_personalization_tags group by 1)
        having cast(total_misdn as float)/total>=0.0001
        order by 4 desc
    )
    , details as 
    (
        select bannerid, vckeyvalue
        from digital.fdn_banner_details
        where vckeyname='TITLE' 
        group by 1,2
    )
select finale.*, '|', details.*
from finale left join details on replace(replace(page,'Browser_',''),'_',' ') = vckeyvalue
where bannerid is not null
```

After the tagging is one, push to table `digital.cst_myIM3_tags`. Current table does not have banner_id/page_id, add it if possible. 

Run below algo to get all the relevant users:

```
drop table if exists digital.PN_personalization_tmp;

CREATE TABLE digital.PN_personalization_tmp AS  
with interests as
    (
        select logs.msisdn, tags.tags, count(*) as n_interests
        FROM digital.fdn_user_journey_myim3 logs inner join digital.cst_myIM3_tags tags on lower(logs.page)=lower(tags.page)
        where trx_date>=from_timestamp(current_date()- interval 30 days, 'yyyyMMdd') and lower(logs.page) like '%browser%'
                -- and logs.msisdn='6285874156629'
        group by 1,2
        order by 3 desc
    )
    , offers as 
    ( 
        select page, tags as offers_tags
        from digital.PN_personalization_tags
        where bannerid='1034' -- sampe banner 
        group by 1,2
    )
    , match_ as 
    (
        select *
        from interests left join offers on interests.tags=offers.offers_tags
    )
    , finale as 
    (
        select msisdn, page, sum(n_interests) as total_interests, count(distinct tags) as n_tags
            , percent_rank() over(order by sum(n_interests) asc) as pctg_rank_n_interests
            , percent_rank() over(order by count(distinct tags) asc) as pctg_rank_n_tags
        from match_
        where page is not null 
        group by 1,2
    )
select *, row_number() over(order by (0.5*pctg_rank_n_tags + 0.5*pctg_rank_n_interests) desc) priority
from finale;


-- add PN_personalization if possible
insert into digital.PN_personalization partition(partition_date)
select 'myIM3' as app, *, from_timestamp(cast('2023-03-13' as timestamp), 'yyyy-MM-dd') as partition_date -- partition_date is the date when the PN will be run 
from digital.PN_personalization_tmp;

select count(*)
from digital.PN_personalization
where partition_date='2023-03-13';

-- Save this file as myIM3.csv
select msisdn
from digital.PN_personalization
where partition_date='2023-03-13';
group by 1
```

Run below script to upload automatically to clevertap
```
import os
import pandas as pd
from datetime import date, datetime
import json
from pathlib import Path

now = datetime.now()
dt_string = now.strftime("%Y%m%d%H%M%S")
segment_name = dt_string

data = pd.read_csv('myIM3.csv')

data['identity'] = data['msisdn']
data[segment_name] = data['msisdn']

manifest = """{
                    "fileName": "CT_segment.csv",
                    "type": "profile",
                    "columns": {
                        "identity": {
                            "ctName": "identity",
                                "dataType": "STRING"
                        },
                        "today_str_col": {
                            "ctName": "today_str_col",
                                "dataType": "STRING"
                        },
                },
                    "clientEmail": "chandra.tjhong@ioh.co.id"
                }"""

manifest = manifest.replace('CT_segment','myIM3_PN_segment')
manifest = manifest.replace('today_str_col',segment_name)


f = open(f"{segment_name}.manifest", "w")
f.write(manifest)
f.close()

relative = Path(f"myIM3_PN_segment.csv")
full_path_csv = relative.absolute()
full_path_csv = str(full_path_csv)

relative = Path(f"{segment_name}.manifest")
full_path_manifest = relative.absolute()
full_path_manifest = str(full_path_manifest)


txt = """#!/bin/bash\nsftp -i ~/.ssh/id_rsa 1527674752@eu1.sftp.clevertap.com <<END\nput csv_file\nput manifest_file\nEND"""
txt = txt.replace('csv_file',f'{full_path_csv}')
txt = txt.replace('manifest_file',f'{full_path_manifest}')

f = open("run.sh", "w")
f.write(txt)
f.close()

relative = Path(f"run.sh")
full_path_sh = relative.absolute()
full_path_sh = str(full_path_sh)

# belum dicoba ya, Dica. tp harunsya bener sih
os.system("chmod +x /home/cst/Indosat/personalization/PN/run.sh")

# belum dicoba ya, Dica. tp harunsya bener sih
os.system("./run.sh")

# clue for Anita -> kirim via email ke stackholders
print(f"clue for Anita: {segment_name}" 

```






