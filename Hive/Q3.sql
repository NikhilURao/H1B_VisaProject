--3.Which industry(SOC_NAME) has the most number of Data Scientist positions?

select lower(soc_name),count(lower(soc_name)) as count from h1b_final where job_title like '%DATA SCIENTIST%' group by lower(soc_name) order by count desc limit 5;
