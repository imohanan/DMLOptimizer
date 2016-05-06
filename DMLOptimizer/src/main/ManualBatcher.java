package main;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import com.mysql.jdbc.Statement;

import model.DML;
import model.DMLQueue;
import model.DMLType;
import util.ManualBatching;

public class ManualBatcher extends Batcher{

	public ManualBatcher(){
		startTime = System.currentTimeMillis();
	}
	
	
	public void BatchAndPush(PriorityQueue<DML> affectedDMLs) throws SQLException {
		batchCalls ++;
		if (minCombinerToBatchSize > affectedDMLs.size())
			minCombinerToBatchSize = affectedDMLs.size();
		if(maxCombinerToBatchSize < affectedDMLs.size())
			maxCombinerToBatchSize = affectedDMLs.size();
		
		Statement manualStatement=(Statement) MySqlSchemaParser.db_conn
				.createStatement();
		List<DML> DMLsToBatch = new LinkedList<DML>();
		Boolean wasPreviousDMLFence = false;
		
		while( affectedDMLs.isEmpty() == false )
		{
			DML currDML = affectedDMLs.remove();
			
			if((DMLsToBatch.isEmpty() == true 
					|| checkBatchingRules(currDML.type, currDML.table, DMLsToBatch.get(0).type, DMLsToBatch.get(0).table) == true)
					&& currDML.IsRecordLevelFence == false
					&& currDML.IsTableLevelFence == false
					&& currDML.type != DMLType.UPDATE
					&& wasPreviousDMLFence == false) //Not attempting to batch update DMLs
			{
				DMLsToBatch.add(currDML);
				wasPreviousDMLFence = currDML.IsRecordLevelFence || currDML.IsTableLevelFence;
			}
			else
			{
				if (DMLsToBatch.isEmpty() == false)
				{
					String batchedStatement = getBatchedStatement(DMLsToBatch);
					manualStatement.addBatch(batchedStatement);
				}
				DMLsToBatch = new LinkedList<DML>();
				DMLsToBatch.add(currDML);
				wasPreviousDMLFence = currDML.IsRecordLevelFence || currDML.IsTableLevelFence;
			}
		}
		
		if (DMLsToBatch.isEmpty() == false)
		{
			String batchedStatement = getBatchedStatement(DMLsToBatch);
			manualStatement.addBatch(batchedStatement);
		}
		 
		int[] results = manualStatement.executeBatch();
		manualStatement.clearBatch();
		manualStatement.close();
		dbmsAccess ++;
		
	}

	@Override
	public void BatchAndPush() throws SQLException {
		Combiner.PKValuesMap.clear();
		Combiner.FKValuesMap.clear();
		
		batchCalls ++;
		if (minCombinerToBatchSize > DMLQueue.queueSize)
			minCombinerToBatchSize = DMLQueue.queueSize;
		if(maxCombinerToBatchSize < DMLQueue.queueSize)
			maxCombinerToBatchSize = DMLQueue.queueSize;
		
		Statement manualStatement=(Statement) MySqlSchemaParser.db_conn
				.createStatement();
		List<DML> DMLsToBatch = new LinkedList<DML>();
		Boolean wasPreviousDMLFence = false;
		
		while( DMLQueue.IsEmpty() == false )
		{
			DML currDML = DMLQueue.RemoveDMLfromHead();
			
			if((DMLsToBatch.isEmpty() == true 
					|| checkBatchingRules(currDML.type, currDML.table, DMLsToBatch.get(0).type, DMLsToBatch.get(0).table) == true)
					&& currDML.IsRecordLevelFence == false
					&& currDML.IsTableLevelFence == false
					&& currDML.type != DMLType.UPDATE
					&& wasPreviousDMLFence == false) //Not attempting to batch update DMLs
			{
				DMLsToBatch.add(currDML);
				wasPreviousDMLFence = currDML.IsRecordLevelFence || currDML.IsTableLevelFence;
			}
			else
			{
				if (DMLsToBatch.isEmpty() == false)
				{
					String batchedStatement = getBatchedStatement(DMLsToBatch);
					manualStatement.addBatch(batchedStatement);
				}
				
				DMLsToBatch = new LinkedList<DML>();
				DMLsToBatch.add(currDML);
				wasPreviousDMLFence = currDML.IsRecordLevelFence || currDML.IsTableLevelFence;
			}
		}
		
		if (!DMLsToBatch.isEmpty())
		{
			String batchedStatement = getBatchedStatement(DMLsToBatch);
			manualStatement.addBatch(batchedStatement);
		}
		
		int[] results = manualStatement.executeBatch();
		manualStatement.clearBatch();
		manualStatement.close();
		dbmsAccess++;

		
	}

	@Override
	public void printStats() {
		float avgbatch ;
		double elapsedTime = (((stopTime - startTime)*1.67)/100000);
		System.out.println("Manual Batching");
	    System.out.println("Time taken in Optimized algorithm: " + elapsedTime +" minutes");
	    
		System.out.println("Total Number of DMLs: " + DMLTotal);
		System.out.println("Number of DMLs after combining: " + DMLAfterCombining);
		
		System.out.println("1. Combiner Stats");
		System.out.println("Table Level Fence count is: " + tableFenceCount);
    	System.out.println("Record Level Fence count is: " + recordFenceCount );
    	
    	System.out.println("Rule InsertUpdate count is : " + insertUpdateCount);
    	System.out.println("Rule InsertDelete count is : " + insertDeleteCount);
    	System.out.println("Rule UpdateDelete count is : " + updateDeleteCount);
    	System.out.println("Rule UpdateUpdate count is : " + updateUpdateCount);
    	System.out.println("Number of  Mathematical Operation type DMLs observed and combined is : " + pendcountDML);
    	
    	System.out.println("2. Batcher Stats");
    	System.out.println("Number of times batcher gets called = Number of times DBMS gets accessed = : " +Integer.toString(batchCalls));
    	System.out.println("Minimum number of DMLs passed from Combiner -> Batcher in one call: "+Integer.toString(minCombinerToBatchSize));
    	System.out.println("maximum number of DMLs passed from Combiner -> Batcher in one call: "+Integer.toString(maxCombinerToBatchSize));
    	System.out.println("Average number of DMLs passed from Combiner -> Batcher per call: "+Float.toString((float)DMLAfterCombining/(float)batchCalls));
    	
    	System.out.println("Minimum number of DMLS in a batch: "+Integer.toString(minBatched));
		System.out.println("Maximum number of DMLS in a batch: "+Integer.toString(maxBatched));
		avgbatch = (float)DMLAfterCombining/(float)countManualBatcher;	
		System.out.println("Average number of DMLs per batch:" + Float.toString(avgbatch));
		System.out.println("Number of DBMS Access:" + Float.toString(dbmsAccess));
		
	}

	private String getBatchedStatement(List<DML> DMLsToBatch) {
		countManualBatcher ++;
		if (DMLsToBatch.size() > maxBatched) 
			maxBatched = DMLsToBatch.size();
		if (DMLsToBatch.size() < minBatched) 
			minBatched = DMLsToBatch.size();
		
		if (DMLsToBatch.size() == 1)
			return DMLsToBatch.get(0).DMLString;
		if(DMLsToBatch.get(0).type == DMLType.INSERT)
		{
			String batchedStatement = ManualBatching.batchInsert(DMLsToBatch);
			return batchedStatement;
		}
		else if(DMLsToBatch.get(0).type == DMLType.DELETE)
		{
			String batchedStatement = ManualBatching.batchDelete(DMLsToBatch);
			return batchedStatement;
		}
		return null;
	}
}
