use h1bproject;

drop table t2011;
drop table t2012;
drop table t2013;
drop table t2014;
drop table t2015;
drop table t2016;
drop table tjoin;
drop table tavg;

create table t2011 (job_title string,count int )
row format delimited
fields terminated by '\t'
stored as textfile;
create table t2012 (job_title string,count int )
row format delimited
fields terminated by '\t'
stored as textfile;
create table t2013 (job_title string,count int )
row format delimited
fields terminated by '\t'
stored as textfile;
create table t2014 (job_title string,count int )
row format delimited
fields terminated by '\t'
stored as textfile;
create table t2015 (job_title string,count int )
row format delimited
fields terminated by '\t'
stored as textfile;
create table t2016 (job_title string,count int )
row format delimited
fields terminated by '\t'
stored as textfile;

 INSERT OVERWRITE TABLE t2011 select job_title, count(case_status) as count from h1b where year=2011 group  by job_title order by count ;
 INSERT OVERWRITE TABLE t2012 select job_title, count(case_status) as count from h1b where year=2012 group  by job_title order by count ;
 INSERT OVERWRITE TABLE t2013 select job_title, count(case_status) as count from h1b where year=2013 group  by job_title order by count ;
 INSERT OVERWRITE TABLE t2014 select job_title, count(case_status) as count from h1b where year=2014 group  by job_title order by count ;
 INSERT OVERWRITE TABLE t2015 select job_title, count(case_status) as count from h1b where year=2015 group  by job_title order by count ;
 INSERT OVERWRITE TABLE t2016 select job_title, count(case_status) as count from h1b where year=2016 group  by job_title order by count ;

create table tjoin (job_title string,count11 int,count12 int,count13 int,count14 int,count15 int,count16 int)
row format delimited
fields terminated by '\t'
stored as textfile;

INSERT OVERWRITE TABLE tjoin select t2011.job_title,t2011.count,t2012.count,t2013.count,t2014.count,t2015.count,t2016.count from t2011 JOIN t2012 on(t2011.job_title=t2012.job_title) JOIN t2013 on(t2012.job_title=t2013.job_title) JOIN t2014 on(t2013.job_title=t2014.job_title) JOIN t2015 on(t2014.job_title=t2015.job_title) JOIN t2016 on(t2015.job_title=t2016.job_title);

create table tavg (job_title string,g1 int,g2 int,g3 int,g4 int,g5 int)
row format delimited
fields terminated by '\t'
stored as textfile;

INSERT OVERWRITE TABLE tavg select job_title,(((count12-count11)/count11)*100)as one,(((count13-count12)/count12)*100)as two,(((count14-count13)/count13)*100)as three,(((count15-count14)/count14)*100)as four,(((count16-count15)/count15)*100)as four from tjoin;

--select job_title, ((g1+g2+g3+g4+g5)/5) as avg from tavg order by avg desc limit 5;
select job_title, ((g1+g2+g3+g4+g5)/5) as avg from tavg order by avg desc limit 5;

