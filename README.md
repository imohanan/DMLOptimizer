# DMLOptimizer

A] The DML Optimizer can be run in 3 modes - Original Run mode, Prepared Statement mode and Manual Batching mode.

Running the code
1. Prepared Statement Mode
  This can be run from the source folder main/Main.java .
  Argument List:[db_UserName db_Password dbName logFile_path true]
  As output the 
  -system-stats.txt
  -queries.txt
  -prepared_accuracy_[dbName].txt 
  files are created. 

2. Manual Batching Mode
  This can be run from the source folder main/Main.java .
  Argument List:[db_UserName db_Password db_name logFile_path false]
  As output the 
  -system-stats.txt
  -queries.txt
  -manual_accuracy_[dbName].txt 
  files are created. 

3. Original Mode
  (This mode should be run after Prepared Statement or Manual mode because it requires the queries.txt to be generated previously.)
  This can be run from the source folder test/OriginalRun.java .
  Argument List:[db_UserName db_Password db_name logFile_path]
  As output the
  -system-orig.txt
  -original_accuracy_[dbName].txt
  files are created. 


B] Other Components
1. system-stats.txt / system-orig.txt
  This file contains system statistics from every 10 seconds.
  It's of the format Available Memory, Percentage Of CPU Used, Average Disk Queue.
2. queries.txt
  This contains a list of all queries to be fired against the database, at the end of the run, in order to check accuracy of the system.
3. prepared_accuracy_[dbName].txt / manual_accuracy_[dbName].txt / orig_accuracy_[dbName].txt 
  This file contains the queries from queries.txt and associated output form dbms. To check if the system is correct, we run util/VerifyAccuracy.java with arguments [dbName, original, prepared, manual] 
