--2 a) Which part of the US has the most Data Engineer jobs for each year?

drop table q2a;

create table q2a (year STRING,worksite STRING,count INT)
row format delimited 
fields terminated by '\t'
stored as textfile;

INSERT overwrite table q2a select year,split(worksite,'[,]')[1] as state, count(split(worksite,'[,]')[1]) as job_count from h1b_final where job_title LIKE '%DATA ENGINEER%' group by split(worksite,'[,]')[1],year order by job_count;

select year,max(struct(count,worksite)).col1 as name1,max(struct(count,worksite)).col2 as count1 from q2a group by year;
