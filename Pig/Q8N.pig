--8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order.


h1b= load '/user/hive/warehouse/h1b_final' using PigStorage('\t') as
(s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,time_position:chararray,wage:int,year:chararray,work_site:chararray,longitute:double,latitute:double);

--describe h1b;

--dump h1b;

filterh1b =FILTER h1b by time_position MATCHES 'N';
--dump filterh1b;

grouph1b = GROUP filterh1b by (year,job_title);

--describe grouph1b;

--dump grouph1b;


reduceh1b= foreach grouph1b generate group as job_title,AVG(filterh1b.wage);
--describe reduceh1b;

dump reduceh1b;

orderh1b = ORDER reduceh1b by $1 desc;
--describe orderh1b;
dump orderh1b;


top10= limit orderh1b 10;

dump top10;
