#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #white
    NUMBER=`echo "\033[33m"` 
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU} ---------------- ${ENTER_LINE} H1B VISA APPLICANTS DATA ANALYSIS PROJECT MENU ${MENU}------------------${NORMAL}"


    echo -e "${MENU} *${NUMBER} 1a${MENU} Is the number of petitions with Data Engineer job title increasing over time? ${NORMAL}"
    echo -e "${MENU} *${NUMBER} 1b${MENU} Find top 5 job titles who are having highest avg growth in applications ${NORMAL}"
    echo -e "${MENU} *${NUMBER} 2a${MENU} Which part of the US has the most Data Engineer jobs for each year? ${NORMAL}"
    echo -e "${MENU} *${NUMBER} 2b${MENU} Find top 5 locations in the US who have got certified visa for each year${NORMAL}"
    echo -e "${MENU} *${NUMBER} 3 ${MENU} Which industry(SOC_NAME) has the most number of Data Scientist positions? ${NORMAL}"
    echo -e "${MENU} *${NUMBER} 4 ${MENU} Which top 5 employers file the most petitions each year?${NORMAL}"
    echo -e "${MENU} *${NUMBER} 5 ${MENU} Find the most popular top 10 job positions for H1B visa applications for each year${NORMAL}"
    echo -e "${MENU} *${NUMBER} 6 ${MENU} Find the percentage and the count of each case status on total applications for each year. ${NORMAL}"
    echo -e "${MENU} *${NUMBER} 7 ${MENU} Create a bar graph to depict the number of applications for each year${NORMAL}"
    echo -e "${MENU} *${NUMBER} 8 ${MENU} Find the average Prevailing Wage for each Job for each Year${NORMAL}"
    echo -e "${MENU} *${NUMBER} 9 ${MENU} Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. ${NORMAL}"
    echo -e "${MENU} *${NUMBER}10 ${MENU} Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions ${NORMAL}"
    echo -e "${MENU} *${NUMBER}11 ${MENU} Export result for question no 10 to MySql database.${NORMAL}"
    echo -e "${MENU} -----------------------${ENTER_LINE} Please enter a menu option ${MENU}------------------${NORMAL}"
    echo -e "${RED_TEXT}Press enter to exit. ${NORMAL}"
    read opt
}
function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}

function getpinCodeBank(){
	echo "in getPinCodebank"
	echo $1
	echo $2
	#hive -e "Select * from menu where PinCode = $1 AND Bank = '$2'"
}

clear
show_menu
while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1a) clear;
        option_picked "Question selected is,1a-Is the number of petitions with Data Engineer job title increasing over time?";
	option_picked "USING HIVE";
        hive -f Q1A.sql
        show_menu;
        ;;

	1b) clear;
	option_picked "Question selected is,1b-Find top 5 job titles who are having highest avg growth in applications";
        option_picked "USING HIVE";
        hive -f Q1B.sql
        show_menu;
        ;;

        2a) clear;
	option_picked "Question selected is,2a-Which part of the US has the most Data Engineer jobs for each year?";        
	option_picked "USING HIVE";
        hive -f Q2A.sql
        show_menu;
        ;;
	
	2b) clear;
        option_picked "Question selected is,2b-Find top 5 locations in the US who have got certified visa for each year";
	option_picked "USING HIVE";
        hive -f Q2B.sql
        show_menu;
        ;;
            
        3) clear;
        option_picked "Question selected is,3-Which industry(SOC_NAME) has the most number of Data Scientist positions?";
	option_picked "USING HIVE";
        hive -f Q3.sql
        show_menu;
        ;;
	
        4) clear;
        option_picked "Question selected is,4-Which top 5 employers file the most petitions each year?"
	option_picked "USING HIVE";
        hive -f Q4.sql 
        show_menu;
        ;;
            
	5) clear;
        option_picked "Find the most popular top 10 job positions for H1B visa applications for each year";
       	option_picked "USING PIG";
        echo -e "${MENU}Select one of the options from below ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 1)${MENU} All Applications ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 2)${MENU} Only Certified Applications ${NORMAL}"
        
	    read n
	    case $n in
                1)                    	
		 pig -x mapreduce Q5A.pig	
		show_menu;	
		;;
                    
                2) 	
                pig -x mapreduce Q5B.pig
                show_menu;
                ;;

	*) echo "Please Select one among the two options[1 or 2]";;
                esac
                show_menu;
                  ;;
                    
        6) clear;
        option_picked "Find the percentage and the count of each case status on total applications for each year."
	option_picked "USING PIG";
        pig -x mapreduce Q6.pig;
        show_menu;
        ;;
        
        7) clear;
	option_picked "Create a bar graph to depict the number of applications for each year"
	option_picked "USING MAPREDUCE";
	hadoop fs -rmr /niit;
        hadoop jar que7.jar q7 /user/hive/warehouse/h1b_final /niit/output7;
	hadoop fs -cat /niit/output7/p*;
        show_menu;
        ;;
        
	    8) clear;
        option_picked "Find the average Prevailing Wage for each Job for each Year (take part time and full time separate)";
        option_picked "USING PIG";
        echo -e "${MENU}Select One Option From Below ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 1)${MENU} Full-Time ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 2)${MENU} Part-Time ${NORMAL}"
        
	    read n
	    case $n in
                1)                    	
		 pig -x mapreduce Q8Y.pig;	
		show_menu;	
		;;
                    
                2) 	
                pig -x mapreduce Q8N.pig
                show_menu;
                ;;

	*) echo "Please Select one among the option[1 or 2]";;
                esac
                show_menu;
                  ;;

	9) clear;
	option_picked "Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions."
	option_picked "USING MAPREDUCE";
 	hadoop fs -rmr /niit;       
	hadoop jar que9.jar q9 /user/hive/warehouse/h1b_final /niit/output9;
	hive -f Q9.sql;
	#hadoop fs -cat /niit/output9/p*;
        show_menu;
        ;;

	10) clear;
	option_picked "Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions."
	option_picked "USING MAPREDUCE";
	hadoop fs -rmr /niit;
        hadoop jar que10.jar q10 /user/hive/warehouse/h1b_final /niit/output10;
	hive -f Q10.sql;	
	#hadoop fs -cat /niit/output10/p*;
        show_menu;
        ;;

	
	11) clear;
	option_picked "Export result for question no 10 to MySql database"
	option_picked "USING SQOOP";
        bash Q11.sh;
        show_menu;
        ;;

        \n) exit;
        ;;

        *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi



done

