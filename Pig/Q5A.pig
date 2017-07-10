--5) Find the most popular top 10 job positions for H1B visa applications for each year?

h1b= load '/user/hive/warehouse/h1b_final' using PigStorage('\t') as
(s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,time_position:chararray,wage:int,year:chararray,work_site:chararray,longitute:double,latitute:double);

--describe h1b;

--dump h1b;

year_jobtitle= group h1b by (year,job_title);
describe year_jobtitle;
--dump year_jobtitle;

foreachyear_jobtitle= foreach year_jobtitle GENERATE flatten(group),COUNT_STAR(h1b.case_status) as count;
--describe foreachyear_jobtitle;
--dump foreachyear_jobtitle;


--dump foreachyear;
store foreachyear_jobtitle into '/niit/h1b5' using PigStorage(',');
h1btop10= load '/niit/h1b5/part-r-00000' using PigStorage(',') as (year:chararray,name:chararray,count:int);
--dump h1btop10;

h1bgroup= group h1btop10 by year;
--dump h1bgroup;
top10= foreach h1bgroup {
sort= order h1btop10 by count desc;
top= limit sort 10;
generate flatten(top);
};

dump top10;
