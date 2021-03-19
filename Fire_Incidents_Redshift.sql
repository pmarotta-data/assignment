-- Create schema to logically group fire incidents tables
CREATE schema fire_incidents authorization awsuser;

-- Create incidents raw data table where source CSV file will be loaded
CREATE TABLE fire_incidents.fire_incidents_raw
(
  "incident number" VARCHAR(20) NOT NULL,
  "exposure number"	INTEGER NOT NULL,
  id INTEGER NOT NULL,
  address VARCHAR(100) NOT NULL,
  "incident date" VARCHAR(20) NOT NULL,
  "call number" VARCHAR(20) NOT NULL,
  "alarm dttm" VARCHAR(20) NOT NULL,
  "arrival dttm" VARCHAR(20) NOT NULL,
  "close dttm" VARCHAR(20) NOT NULL,
  city VARCHAR(20) NOT NULL,
  zipcode VARCHAR(20) NOT NULL,
  battalion	VARCHAR(20) NOT NULL,
  "station area" VARCHAR(20) NOT NULL,
  box VARCHAR(100) NULL,
  "suppression units" INTEGER NOT NULL,
  "suppression personnel" INTEGER NOT NULL,
  "ems units" INTEGER NOT NULL,
  "ems personnel" INTEGER NOT NULL,
  "other units"	INTEGER NOT NULL,
  "other personnel"	INTEGER NOT NULL,
  "first unit on scene"	VARCHAR(50) NOT NULL,
  "estimated property loss"	INTEGER NULL,
  "estimated contents loss" FLOAT NULL,
  "fire fatalities"	INTEGER NOT NULL,
  "fire injuries" INTEGER NOT NULL,
  "civilian fatalities"	INTEGER NOT NULL,
  "civilian injuries" INTEGER NOT NULL,
  "number of alarms" INTEGER NOT NULL,
  "primary situation" VARCHAR(200) NOT NULL,
  "mutual aid" VARCHAR(200) NOT NULL,
  "action taken primary" VARCHAR(200) NOT NULL,
  "action taken secondary" VARCHAR(200) NOT NULL,
  "action taken other"	VARCHAR(200) NOT NULL,
  "detector alerted occupants" VARCHAR(200) NOT NULL,
  "property use" VARCHAR(200) NOT NULL,
  "area of fire origin"	VARCHAR(200) NOT NULL,
  "ignition cause"	VARCHAR(200) NULL,
  "ignition factor primary"	VARCHAR(200) NULL,
  "ignition factor secondary" VARCHAR(200) NULL,
  "heat source"	VARCHAR(200) NULL,
  "item first ignited" VARCHAR(200) NULL,
  "human factors associated with ignition" VARCHAR(200) NULL,
  "structure type" VARCHAR(50) NULL,
  "structure status" VARCHAR(50) NULL,
  "floor of fire origin" INTEGER NULL,
  "fire spread"	VARCHAR(50) NULL,
  "no flame spead" VARCHAR(50) NULL,
  "number of floors with minimum damage" INTEGER NULL,
  "number of floors with significant damage" INTEGER NULL,
  "number of floors with heavy damage"	INTEGER NULL,
  "number of floors with extreme damage" INTEGER NULL,
  "detectors present" VARCHAR(50) NULL,
  "detector type" VARCHAR(50) NULL,
  "detector operation"	VARCHAR(50) NULL,
  "detector effectiveness"	VARCHAR(200) NULL,
  "detector failure reason"	VARCHAR(200) NULL,
  "automatic extinguishing system present"	VARCHAR(200) NULL,
  "automatic extinguishing sytem type"	VARCHAR(200) NULL,
  "automatic extinguishing sytem perfomance" VARCHAR(200) NULL,
  "automatic extinguishing sytem failure reason" VARCHAR(200) NULL,
  "number of sprinkler heads operating"	INTEGER NULL,
  "supervisor district"	VARCHAR(50) NULL,
  neighborhood_district VARCHAR(50) NOT NULL,
  point VARCHAR(50) NOT NULL
);

-- Load raw data from data lake bucket on S3
copy fire_incidents.fire_incidents_raw
from 's3://incidents-raw/Fire_Incidents.csv' 
iam_role 'arn:aws:iam::028974180601:role/RedShiftFullAccessS3' 
csv
ignoreheader 1;

-- Grant access to awsuser to sys error logs to troubleshoot and debug data loading errors
alter user awsuser SYSLOG ACCESS UNRESTRICTED

-- Check for errors during data load
select distinct tbl, query, starttime,
trim(filename) as input, line_number, colname, err_code,
trim(err_reason) as reason
from stl_load_errors sl
order by starttime

-- Explore data to be used on dimension district                                               
select distinct neighborhood_district
from fire_incidents.fire_incidents_raw

-- Explore data to be used on dimension battalion
select distinct battalion
from fire_incidents.fire_incidents_raw

-- Create dimension table for District
create table fire_incidents.dim_district(districtid int identity(1, 1),
neighborhood_district VARCHAR(50), primary key(districtid));

-- Populate dimension table from source raw data
insert into fire_incidents.dim_district(neighborhood_district)
select distinct neighborhood_district 
from fire_incidents.fire_incidents_raw
where neighborhood_district is not null;

-- Explore and validate district dimension data
select * from fire_incidents.dim_district order by districtid
 
-- Create dimension table for Battalion                                              
create table fire_incidents.dim_battalion(battalionid int identity(1, 1),
battalion VARCHAR(20), primary key(battalionid));

-- Populate battalion dimension table from source raw data                                   
insert into fire_incidents.dim_battalion(battalion)
select distinct battalion 
from fire_incidents.fire_incidents_raw
where battalion is not null;
                                   
-- Explore and validate district dimension battalion
select * from fire_incidents.dim_battalion order by battalionid

-- Add battalion dimension id to raw data table (later this will act as FK to the dimension)
alter table fire_incidents.fire_incidents_raw
add column battalionid int
default NULL;

-- Add district dimension id to raw data table (later this will act as FK to the dimension)
alter table fire_incidents.fire_incidents_raw
add column districtid int
default NULL;

-- Add battalion ids from battalion dimension table into raw data                                   
update fire_incidents.fire_incidents_raw 
set battalionid = dim_battalion.battalionid
from fire_incidents.dim_battalion 
where fire_incidents_raw.battalion = dim_battalion.battalion
;

-- Explore and validate Battalion ids added to raw data                           
select distinct battalion, battalionid from fire_incidents.fire_incidents_raw order by battalionid
                                   
-- Add district ids from district dimension table into raw data                                   
update fire_incidents.fire_incidents_raw 
set districtid = dim_district.districtid
from fire_incidents.dim_district 
where fire_incidents_raw.neighborhood_district = dim_district.neighborhood_district
;
                                   
-- Explore and validate District ids added to raw data                           
select distinct neighborhood_district, districtid from fire_incidents.fire_incidents_raw order by districtid;

-- Add alarm time only column                                   
alter table fire_incidents.fire_incidents_raw
add column alarm_tm time NULL;

-- Create staging table to process the whole alarm datetime data and extract time data
select "incident number", convert(time,split_part("alarm dttm",'T',2)) as alarm_tm 
into fire_incidents.temp_alarm_tm                               
from fire_incidents.fire_incidents_raw
where "alarm dttm" is not null;                                   

-- Add separate columns to store hour, minutes and seconds to the staging table
alter table fire_incidents.temp_alarm_tm add column alarm_hr INTEGER null;                                   alter table fire_incidents.temp_alarm_tm add column alarm_mm INTEGER null;
alter table fire_incidents.temp_alarm_tm add column alarm_ss INTEGER null;                                   
                                                  
-- Validate split and convert functions before proceeding                                                  
select top 5 convert(INTEGER,split_part(alarm_tm,':',1)) as alarm_hr
from fire_incidents.temp_alarm_tm

-- Populate separate columns for hour, minutes and seconds on the staging table
update fire_incidents.temp_alarm_tm 
set alarm_hr = convert(char(2),split_part(alarm_tm,':',1)),                                           			alarm_mm = convert(char(2),split_part(alarm_tm,':',2)),
	alarm_ss = convert(char(2),split_part(alarm_tm,':',3))
where alarm_tm is not null                                          

-- Validate how temp table is looking at this point                                           
select top 1000 * from fire_incidents.temp_alarm_tm                               

-- Add columns to store alarm hour, minutes and seconds on the source raw table
alter table fire_incidents.fire_incidents_raw add column alarm_hr INTEGER null;                               alter table fire_incidents.fire_incidents_raw add column alarm_mm INTEGER null;
alter table fire_incidents.fire_incidents_raw add column alarm_ss INTEGER null;                               

-- Populate columns on source raw data from pre calculated columns from staging table
update fire_incidents.fire_incidents_raw 
set alarm_tm = temp_alarm_tm.alarm_tm,
	alarm_hr = temp_alarm_tm.alarm_hr,
    alarm_mm = temp_alarm_tm.alarm_mm,
    alarm_ss = temp_alarm_tm.alarm_ss                                      
from fire_incidents.temp_alarm_tm 
where fire_incidents_raw."incident number" = temp_alarm_tm."incident number"
AND temp_alarm_tm.alarm_tm is not null
;                                                  

-- Create dimension table to store alarm hour, minutes and seconds                                          
create table fire_incidents.dim_alarm_tm (alarm_tm_id INTEGER identity(1, 1),
alarm_tm time, alarm_hr INTEGER, alarm_mm INTEGER, alarm_ss INTEGER, primary key(alarm_tm_id));

-- Populate alarm time dimension table with existing values from source raw data              
insert into fire_incidents.dim_alarm_tm(alarm_tm, alarm_hr, alarm_mm, alarm_ss)
select distinct alarm_tm, alarm_hr, alarm_mm, alarm_ss 
from fire_incidents.fire_incidents_raw
where alarm_tm is not null
order by alarm_hr, alarm_mm, alarm_ss                                                                         

-- Add Alarm time id column from dimension table into raw data table (this will be FK later)
alter table fire_incidents.fire_incidents_raw add column alarm_tm_id int null;
                                                                                 
-- Populate Alarm time ids from Alarm time dimension table into source raw data
update fire_incidents.fire_incidents_raw 
set alarm_tm_id = dim_alarm_tm.alarm_tm_id
from fire_incidents.dim_alarm_tm 
where fire_incidents_raw.alarm_hr = dim_alarm_tm.alarm_hr
and fire_incidents_raw.alarm_mm = dim_alarm_tm.alarm_mm
and fire_incidents_raw.alarm_ss = dim_alarm_tm.alarm_ss         
;
                                                                          
-- Create fact table for fire incidents with numeric columns and FK's
create table fire_incidents.fact_fire_incidents
(
  "incident number" INTEGER NOT NULL,
  id INTEGER NOT NULL,
  alarm_tm_id INTEGER NOT NULL,
  battalionid INTEGER NOT NULL,
  districtid INTEGER NOT NULL,
  "suppression units" INTEGER NOT NULL,
  "suppression personnel" INTEGER NOT NULL,
  "ems units" INTEGER NOT NULL,
  "ems personnel" INTEGER NOT NULL,
  "other units"	INTEGER NOT NULL,
  "other personnel"	INTEGER NOT NULL,
  "estimated property loss"	INTEGER NULL,
  "estimated contents loss" FLOAT NULL,
  "fire fatalities"	INTEGER NOT NULL,
  "fire injuries" INTEGER NOT NULL,
  "civilian fatalities"	INTEGER NOT NULL,
  "civilian injuries" INTEGER NOT NULL,
  "number of alarms" INTEGER NOT NULL,
  "floor of fire origin" INTEGER NULL,
  "number of floors with minimum damage" INTEGER NULL,
  "number of floors with significant damage" INTEGER NULL,
  "number of floors with heavy damage"	INTEGER NULL,
  "number of floors with extreme damage" INTEGER NULL,
  "number of sprinkler heads operating"	INTEGER NULL,
  primary key("incident number"),
  foreign key(alarm_tm_id) references fire_incidents.dim_alarm_tm(alarm_tm_id),
  foreign key(battalionid) references fire_incidents.dim_battalion(battalionid),
  foreign key(districtid) references fire_incidents.dim_district(districtid))
  distkey(alarm_tm_id)
  compound sortkey(battalionid,districtid);                                                                   
-- Also assigned Distribution Key to the alarm time FK and created compound Sort Keys on the other 2 FK's to optimize query performance                                                                  

-- Populate fire incidents fact table                                                                 
insert into fire_incidents.fact_fire_incidents
 ("incident number",id,alarm_tm_id,battalionid,
  districtid,"suppression units","suppression personnel","ems units",
  "ems personnel","other units","other personnel","estimated property loss",
  "estimated contents loss","fire fatalities","fire injuries",
  "civilian fatalities","civilian injuries","number of alarms",
  "floor of fire origin","number of floors with minimum damage",
  "number of floors with significant damage","number of floors with heavy damage",
  "number of floors with extreme damage","number of sprinkler heads operating")
select convert(integer,"incident number") as "incident number",id,alarm_tm_id,battalionid,
  districtid,"suppression units","suppression personnel","ems units",
  "ems personnel","other units","other personnel","estimated property loss",
  "estimated contents loss","fire fatalities","fire injuries",
  "civilian fatalities","civilian injuries","number of alarms",
  "floor of fire origin","number of floors with minimum damage",
  "number of floors with significant damage","number of floors with heavy damage",
  "number of floors with extreme damage","number of sprinkler heads operating"
from fire_incidents.fire_incidents_raw   

-- Fact table for fire incidents is ready to be consumed by reports now!                        
                                                                
-- Example report #1: Generate a list with the hourly amount of civilian fatalities between 14hs and 22hs
select at.alarm_hr as hourly, sum ("civilian fatalities") as total_civilian_fatalities  
from fire_incidents.fact_fire_incidents as fi
join fire_incidents.dim_alarm_tm as at on fi.alarm_tm_id = at.alarm_tm_id
where at.alarm_hr between 14 and 22
group by at.alarm_hr
order by at.alarm_hr                                                                 

-- Example report #2: Generate a list with the total count of incidents on each District
select di.neighborhood_district, count ("incident number") as total_count_of_incidents   
from fire_incidents.fact_fire_incidents as fi
join fire_incidents.dim_district as di on fi.districtid = di.districtid
group by di.neighborhood_district
order by total_count_of_incidents desc                                                                 
                                            
-- Example report #3: Generate a list of total amount of suppression personnel assigned to battalion B10 ordered by minute at 22hs o'clock.
select ba.battalion, at.alarm_mm as minutes, sum ("suppression personnel") as total_suppression_personnel  
from fire_incidents.fact_fire_incidents as fi
join fire_incidents.dim_alarm_tm as at on fi.alarm_tm_id = at.alarm_tm_id
join fire_incidents.dim_battalion as ba on fi.battalionid = ba.battalionid
where at.alarm_hr = 22 and ba.battalion = 'B10'
group by ba.battalion, at.alarm_mm
order by minutes desc                                                              