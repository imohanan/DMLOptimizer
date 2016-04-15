package main;

import java.sql.SQLException;
import java.util.PriorityQueue;

import com.mysql.jdbc.Statement;

import model.DML;
import model.DMLQueue;
import model.DMLType;
import util.PrepStatement;
import util.Stats;

public class PreparedBatcher extends Batcher{

	int batchSize;
	private Statement statement = null;
	private  String template = null;
	private  java.sql.PreparedStatement preparedStatement = null;
	private  DMLType currType = null;
	private  DML currDML = null;
	private  String currTable = null;
	
	@Override
	public void BatchAndPush(PriorityQueue<DML> affectedDMLs) throws SQLException {
		batchSize = 0;
		batchCalls++;
		MySqlSchemaParser.db_conn.setAutoCommit(false);
		int sizeofheap = affectedDMLs.size();
		//DMLSentToBatcher += sizeofheap;
		if (sizeofheap > maxCombinerToBatchSize) 
			maxCombinerToBatchSize = sizeofheap;
		if (sizeofheap < minCombinerToBatchSize)
			minCombinerToBatchSize = sizeofheap;
		int batchcount =0;
		while (!affectedDMLs.isEmpty()) {
			if (currTable == null && currType == null) {
				currDML = affectedDMLs.remove();
				if ((currDML.type == DMLType.INSERT || currDML.type == DMLType.DELETE) && !currDML.isRecordLevelFence()
						&& !currDML.isTableLevelFence()) {
					currType = currDML.type;
					currTable = currDML.table;
					if (currType == DMLType.INSERT) {
						template = PrepStatement.tableToInsertPreparedStatements.get(currTable);
					} else if (currType == DMLType.DELETE) {
						template = PrepStatement.tableToDeletePreparedStatements.get(currTable);
					}
					preparedStatement = MySqlSchemaParser.db_conn.prepareStatement(template);
					fillPreparedStatement(currTable, currDML);
					preparedStatement.addBatch();

				} else {
					if (preparedStatement != null) 
						executePreparedStatement();
					Statement st = (Statement) MySqlSchemaParser.db_conn.createStatement();
					st.addBatch(currDML.toDMLString());
					st.executeBatch();
					MySqlSchemaParser.db_conn.commit();
					st.clearBatch();
					st.close();
					dbmsAccess++;
					if (minBatched > 1)
						minBatched = 1;
					if (maxBatched < 1)
						maxBatched = 1;
					continue;
					
				}
			} else {
				currDML = affectedDMLs.peek();
				if (checkBatchingRules(currDML.type, currDML.table, currType, currTable)
						&& !currDML.isRecordLevelFence() && !currDML.isTableLevelFence()) {
					currDML = affectedDMLs.remove();
					fillPreparedStatement(currTable, currDML);
					preparedStatement.addBatch();

				} else {
					executePreparedStatement();
				}
			}
		}
		if (preparedStatement != null) {
			executePreparedStatement();
		}

		
	}

	@Override
	public void BatchAndPush() throws SQLException {
		batchSize = 0;
		batchCalls++;
		int sizeofqueue = DMLQueue.getQueueSize();
		//DMLSentToBatcher += sizeofqueue;
		if (sizeofqueue > maxCombinerToBatchSize) 
			maxCombinerToBatchSize = sizeofqueue;
		if (sizeofqueue < minCombinerToBatchSize)
			minCombinerToBatchSize = sizeofqueue;
		Combiner.PKValuesMap.clear();
		MySqlSchemaParser.db_conn.setAutoCommit(false);
		while (!DMLQueue.IsEmpty()) {
			if (currTable == null && currType == null) {
				currDML = DMLQueue.RemoveDMLfromHead();
				if ((currDML.type == DMLType.INSERT || currDML.type == DMLType.DELETE) && !currDML.isRecordLevelFence()
						&& !currDML.isTableLevelFence()) {
					currType = currDML.type;
					currTable = currDML.table;
					if (currType == DMLType.INSERT) {
						template = PrepStatement.tableToInsertPreparedStatements.get(currTable);
					} else if (currType == DMLType.DELETE) {
						template = PrepStatement.tableToDeletePreparedStatements.get(currTable);
					}
					preparedStatement = MySqlSchemaParser.db_conn.prepareStatement(template);
					fillPreparedStatement(currTable, currDML);
					preparedStatement.addBatch();

				} else {
					if (preparedStatement != null) 
						executePreparedStatement();
					Statement st = (Statement) MySqlSchemaParser.db_conn.createStatement();
					st.addBatch(currDML.toDMLString());
					st.executeBatch();
					MySqlSchemaParser.db_conn.commit();
					st.clearBatch();
					st.close();
					dbmsAccess ++;
					continue;
					
				}
			} else {
				currDML = DMLQueue.DMLQueueHead;
				if (checkBatchingRules(currDML.type, currDML.table, currType, currTable)
						&& !currDML.isRecordLevelFence() && !currDML.isTableLevelFence()) {
					currDML = DMLQueue.RemoveDMLfromHead();
					fillPreparedStatement(currTable, currDML);
					preparedStatement.addBatch();

				} else {
					executePreparedStatement();
				}
			}
		}
		if (preparedStatement != null) {
			executePreparedStatement();
		}

		
	}

	@Override
	public void printStats() {
		float avgbatch ;
		double elapsedTime = (((stopTime - startTime)*1.67)/100000);
		System.out.println("Prepared Batching");
	    System.out.println("Time taken in Optimized algorithm: " + elapsedTime +" minutes");
	    
		System.out.println("Total Number of DMLs: " + DMLTotal);
		System.out.println("Number of DMLs after combining: " + DMLAfterCombining);
		
		System.out.println("Table Level Fence count is: " + tableFenceCount);
    	System.out.println("Record Level Fence count is: " + recordFenceCount );
    	
    	System.out.println("Rule InsertUpdate count is : " + insertUpdateCount);
    	System.out.println("Rule InsertDelete count is : " + insertDeleteCount);
    	System.out.println("Rule UpdateDelete count is : " + updateDeleteCount);
    	System.out.println("Rule UpdateUpdate count is : " + updateUpdateCount);
    	System.out.println("Number of  PendCount type DMLs observed and combined is : " + pendcountDML);
    	
    	System.out.println("Number of times batcher gets called: " +Integer.toString(batchCalls));
    	System.out.println("Minimum number of DMLs passed from Combiner -> Batcher in one call: "+Integer.toString(minCombinerToBatchSize));
    	System.out.println("maximum number of DMLs passed from Combiner -> Batcher in one call: "+Integer.toString(maxCombinerToBatchSize));
    	System.out.println("Average number of DMLs passed from Combiner -> Batcher per call: "+Float.toString((float)DMLAfterCombining/(float)batchCalls));
    	
//    	System.out.println("Total number of DMLs after batching: "+ totalBatched);
//		System.out.println("Total number of access to dbms: "+dbmsAccess);
		System.out.println("Minimum number of DMLS in a batch: "+Integer.toString(minBatched));
		System.out.println("Maximum number of DMLS in a batch: "+Integer.toString(maxBatched));
		avgbatch = (float)DMLAfterCombining/(float)dbmsAccess;	
		System.out.println("Average number of DMLs per batch:" + Float.toString(avgbatch));
//		System.out.println("Average number of DMLS in each batch: "+Float.toString(DMLAfterCombining/totalBatched));

		
	}
	
	public void fillPreparedStatement(String table, DML dml) throws SQLException {
		int attrCount = 1;
		for (String attr : MySqlSchemaParser.TableAttrs.get(table)) {// List of
																		// attributes
																		// in
																		// the
																		// table
			String attrType = MySqlSchemaParser.AttrTypes.get(table).get(attr);
			String attrVal = null;
			if (dml.DMLSetAttributeValues.containsKey(attr)) {
				attrVal = dml.DMLSetAttributeValues.get(attr);
			} else {
				attrVal = MySqlSchemaParser.AttrInitVal.get(table).get(attr);
			}
			if (attrType.equalsIgnoreCase("VARCHAR") || attrType.equalsIgnoreCase("LONGVARCHAR"))
				preparedStatement.setString(attrCount, attrVal);
			else if (attrType.equalsIgnoreCase("int"))
				preparedStatement.setInt(attrCount, Integer.parseInt(attrVal));
			if (attrVal.equalsIgnoreCase("null")) {
				if (attrType.equalsIgnoreCase("VARCHAR"))
					preparedStatement.setNull(attrCount, java.sql.Types.VARCHAR);
				else if (attrType.equalsIgnoreCase("LONGVARCHAR"))
					preparedStatement.setNull(attrCount, java.sql.Types.LONGNVARCHAR);
				else if (attrType.equalsIgnoreCase("int"))
					preparedStatement.setNull(attrCount, java.sql.Types.INTEGER);

			}

			attrCount++;
		}
	}
	
	public void executePreparedStatement() throws SQLException {
		int counts[] = preparedStatement.executeBatch();
		int batchsize = counts.length;
		if (batchsize > maxBatched) 
			maxBatched = batchsize;
		if (batchsize < minBatched)
			minBatched = batchsize;
		preparedStatement.clearBatch();
		MySqlSchemaParser.db_conn.commit();
		dbmsAccess ++;
		preparedStatement = null;
		currTable=null;
		currType=null;
	}


}
