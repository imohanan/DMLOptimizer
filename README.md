# DMLOptimizer

The _DML Optimizer_ can be run in 3 modes - Original Run mode, Prepared Statement mode and Manual Batching mode. <br/>
  
**A] Running the code**


1. **Prepared Statement Mode**
  * This can be run from the source folder main/Main.java . <br/>
    Argument List:[db_UserName db_Password dbName logFile_path true]  
  * As output following files are created: 
    * system-stats.txt 
    * queries.txt 
    * prepared_accuracy_[dbName].txt  
    

2. **Manual Batching Mode** 
  * This can be run from the source folder main/Main.java . <br/>
    Argument List:[db_UserName db_Password db_name logFile_path false] <br/>
  * As output following files are created:
    * system-stats.txt 
    * queries.txt 
    * manual_accuracy_[dbName].txt 

3. **Original Mode** 
  (This mode should be run after Prepared Statement or Manual mode because it requires the queries.txt to be generated previously.) <br/>
  * This can be run from the source folder test/OriginalRun.java .<br/>
    Argument List:[db_UserName db_Password db_name logFile_path] 
  * As output following files are created:
    * system-orig.txt 
    * original_accuracy_[dbName].txt 
    
**B] Other Components** 

1. system-stats.txt / system-orig.txt 
  * This file contains system statistics from every 10 seconds. 
  * It's of the format Available Memory, Percentage Of CPU Used, Average Disk Queue 
2. queries.txt
  * This contains a list of all queries to be fired against the database, at the end of the run, in order to check accuracy of the system.
3. prepared_accuracy_[dbName].txt / manual_accuracy_[dbName].txt / orig_accuracy_[dbName].txt 
  * This file contains the queries from queries.txt and associated output form dbms. To check if the system is correct, we   run **util/VerifyAccuracy.java** with arguments [dbName, original, prepared, manual]. See the comments in           util/VerifyAccuracy.java to understand argument list and also to set the path where the files to be checked are stored. <br/>
  

**C] To make DML Optimizer work on Unix based Machine**
  1. Make MySQL DBMS case insensitive by following these steps (By default, MySQL linux distribution are case sensitive)
    1. Open terminal and edit /etc/mysql/my.cnf 
    2. sudo nano /etc/mysql/my.cnf
    3. Underneath the [mysqld] section.add: 
    4. lower_case_table_names = 1 
    5. Restart mysql 
    6. sudo /etc/init.d/mysql restart <br/>
    7. Then check it here in variables, if the lower_case_table_names is successfully added.
    8. mysqladmin -u root -p variables <br/>
  2. Instead of using SystemStats.java (Windows-based) method(Comment it from main/Main.java). Run shell scripts from utilization_scripts folder. Refer to readme.txt given in utilization_scripts folder for further instructions.

