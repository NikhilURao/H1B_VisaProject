 
data = LOAD '/user/hive/warehouse/h1b_final' USING PigStorage('\t') as 
(s_no:int,
case_status:chararray,
employer_name:chararray,
soc_name:chararray,
job_title:chararray,
full_time_position:chararray,
prevailing_wage:int,
year:chararray,
worksite:chararray,
longitute:double,
latitute:double);		


groupbyyear= group data by year;
couneachyear= foreach groupbyyear generate group,COUNT(data.case_status);


groupbyyearcasestatus= group data by (year,case_status);
countyearlycasestatus= foreach groupbyyearcasestatus generate group,group.$0,COUNT($1);
--dump countyearlycasestatus;
joined= join countyearlycasestatus by $1,couneachyear by $0;
ans= foreach joined generate FLATTEN($0),(float)($2*100)/$4,$2; --percent generation
dump ans;

