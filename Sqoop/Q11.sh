#11) Export result for question no 10 to MySql database.

#We first need to create a database by the name q11 and inside q11 a table by the ques11 in mysql.
mysql -u root -p'root' -e 'create database q11;use q11;create table ques11 (job_title varchar(100
),success_rate float);';

#export command(source-mapper output of question 10:destination-hive table ques11)  
sqoop export --connect jdbc:mysql://localhost/q11 --username root --password 'root' --table ques11 --update-mode allowinsert  --export-dir /niit/question10/p* --input-fields-terminated-by '\t';

#to display the contents of table ques11 in database q11
mysql -u root -p'root' -e 'select * from q11.ques11';
