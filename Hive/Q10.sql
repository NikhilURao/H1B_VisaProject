drop table question10;

create table question10(job_name STRING, count INT,count1 INT, per FLOAT)
row format delimited 
fields terminated by '\t'
stored as textfile;

load data inpath '/niit/output10/p*' into table question10;
 select * from question10 order by per desc;
