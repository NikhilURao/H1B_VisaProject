--4.Which top 5 employers file the most petitions each year? 

drop table q4; 

create table q4(emp_name STRING,year STRING,count INT)
row format delimited 
fields terminated by '\t'
stored as textfile;

insert overwrite table q4 select employer_name, year,count(year) as count from h1b_final group by year,employer_name order by count;

--select emp_name,years,max(count) as c from q4 group by emp_name,years order by c desc; 

select * from(select year,emp_name,count,RANK() OVER (partition by year order by count desc) as rank from q4)t where rank<6;
