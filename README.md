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
