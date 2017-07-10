drop table data_engineer;

create table data_engineer (year STRING, count INT)
row format delimited 
fields terminated by '\t'
stored as textfile;

select year, count(case_status) from h1b_final where job_title like '%DATA ENGINEER%' group by year order by year;

insert overwrite table data_engineer select year, count(case_status) from h1b_final where job_title like '%DATA ENGINEER%' group by year order by year;

select a.year,b.year,(((b.count-a.count)/a.count)*100) from data_engineer a, data_engineer b where b.year = a.year+1;
 	
