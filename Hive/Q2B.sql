--2 b) find top 5 locations in the US who have got certified visa for each year.

drop table q2b;

create table q2b (year string,worksite string,count int )
row format delimited
fields terminated by '\t'
stored as textfile; 

INSERT OVERWRITE TABLE q2b select year,split(worksite,'[,]')[1] as state, count(split(worksite,'[,]')[1]) as job_count from h1b_final where case_status LIKE 'CERTIFIED' group by split(worksite,'[,]')[1],year order by job_count DESC;

select * from(select year,worksite,count,RANK() OVER (partition by year order by count desc) as rank from q2b)t where rank<6;
