drop table question9;

create table question9(emp_name STRING, count INT,count1 INT, per FLOAT)
row format delimited 
fields terminated by '\t'
stored as textfile;

load data inpath '/niit/output9/p*' into table question9;
 select * from question9 order by per desc;
