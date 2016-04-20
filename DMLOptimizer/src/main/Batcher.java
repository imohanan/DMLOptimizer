package main;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import model.DML;
import model.DMLType;

public abstract class Batcher {

	public  long startTime = 0;
	public  long stopTime = 0;
	public  double TotalTime = 0;
	
	public  int DMLTotal = 0;
	public  int DMLAfterCombining = 0;
	
	public  int insertUpdateCount = 0;
	public  int insertDeleteCount = 0;
	public  int updateDeleteCount = 0;
	public  int updateUpdateCount = 0;
	public  int pendcountDML = 0 ;
	
	public  int recordFenceCount = 0;
	public  int tableFenceCount = 0;
	public  int totalBatched=1;
	
	public  int maxCombinerToBatchSize=Integer.MIN_VALUE;
	public  int minCombinerToBatchSize=Integer.MAX_VALUE;
	public  int batchCalls=0;

	public  int minBatched= Integer.MAX_VALUE;//Min batched from any size of group coming from combiner
	public  int maxBatched=1;
    
	public  int dbmsAccess=0;
	public  int countManualBatcher=0; 
	public  boolean issueToDBMS=true;
	public  Map<Integer, Integer> NoDMLsPassedToBatcher=new HashMap<Integer,Integer>();//<size of group,occurance>
	public  Map<Integer, Integer> BatchSizeToDBMSAccess=new HashMap<Integer,Integer>();//<size of group,#dbms access>

	public void BatchAndPush(PriorityQueue<DML> affectedDMLs) throws SQLException, ParseException {}
	public  void BatchAndPush() throws SQLException, ParseException {}
	public  void printStats(){}
	
	protected  boolean checkBatchingRules(DMLType dml1Type, String dml1Table, DMLType dml2Type, String dml2Table) {
		if ((dml1Type == dml2Type) && (dml1Table.equals(dml2Table)))
			return true;
		return false;
	}
}
