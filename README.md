# DMLOptimizer <br/>

A] The DML Optimizer can be run in 3 modes - Original Run mode, Prepared Statement mode and Manual Batching mode. <br/>

Running the code <br/>

1. Prepared Statement Mode <br/>
  This can be run from the source folder main/Main.java . <br/>
  Argument List:[db_UserName db_Password dbName logFile_path true]  <br/>
  As output the  <br/>
  -system-stats.txt <br/>
  -queries.txt <br/>
  -prepared_accuracy_[dbName].txt  <br/> 
  files are created.  <br/>

2. Manual Batching Mode <br/>
  This can be run from the source folder main/Main.java . <br/>
  Argument List:[db_UserName db_Password db_name logFile_path false] <br/>
  As output the  <br/>
  -system-stats.txt <br/>
  -queries.txt <br/>
  -manual_accuracy_[dbName].txt <br/>
  files are created. <br/>

3. Original Mode <br/>
  (This mode should be run after Prepared Statement or Manual mode because it requires the queries.txt to be generated previously.) <br/>
  This can be run from the source folder test/OriginalRun.java .<br/>
  Argument List:[db_UserName db_Password db_name logFile_path] <br/>
  As output the <br/>
  -system-orig.txt <br/>
  -original_accuracy_[dbName].txt <br/>
  files are created.  <br/>


B] Other Components <br/>
1. system-stats.txt / system-orig.txt <br/>
  This file contains system statistics from every 10 seconds. <br/>
  It's of the format Available Memory, Percentage Of CPU Used, Average Disk Queue. <br/>
2. queries.txt <br/>
  This contains a list of all queries to be fired against the database, at the end of the run, in order to check accuracy of the system. <br/>
3. prepared_accuracy_[dbName].txt / manual_accuracy_[dbName].txt / orig_accuracy_[dbName].txt  <br/>
  This file contains the queries from queries.txt and associated output form dbms. To check if the system is correct, we run util/VerifyAccuracy.java with arguments [dbName, original, prepared, manual] <br/>

C] To make DML Optimizer work in Linux Machine <br/>
  1) Make MySQL DBMS case insesitive by following these steps:<br/>
  Open terminal and edit /etc/mysql/my.cnf <br/>
  sudo nano /etc/mysql/my.cnf<br/>
  Underneath the [mysqld] section.add: <br/>
  lower_case_table_names = 1 <br/>
  Restart mysql <br/>
  sudo /etc/init.d/mysql restart <br/>
  Then check it here: <br/>
  mysqladmin -u root -p variables <br/>
  
  2)Instead of using SystemStats.java (windows-based) method. Run shell scripts from utilization_scripts folder.
  Refer to readme.txt given in utilization_scripts folder for further instructions.

