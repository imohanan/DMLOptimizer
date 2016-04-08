package util;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.mysql.jdbc.Statement;

import main.Combiner;
import main.MySqlSchemaParser;
import model.DML;
import model.DMLQueue;
import model.DMLType;

public class Util {
	private static Statement statement=null;
	private static DMLType currType = null;
	private static String currTable = null;
	public static int batchSize;

	public static String[] splitDMLsByOR(String dmlString) {
		dmlString = dmlString.replace(";", " ");
		dmlString = dmlString.trim();
		String[] parts = dmlString.split("\\s*(?i)where\\s*");
		if (parts.length == 1)
			return new String[] { dmlString };

		String DMLPre = parts[0];
		String[] DMLPosts = parts[1].split("\\s*(?i)or\\s*");

		String NewDMLs[] = new String[DMLPosts.length];

		for (int idx = 0; idx < DMLPosts.length; idx++) {
			String whereClause = DMLPosts[idx];
			String newDML = DMLPre.trim() + " where " + whereClause.trim()
					+ ";";
			NewDMLs[idx] = newDML;
		}

		return NewDMLs;
	}

	public static void BatchAndPush() throws SQLException {// For
												// TableLevelFence
		batchSize=0;
		Stats.batchCalls++;
		Combiner.PKValuesMap.clear();
		if (statement==null){
		statement=  (Statement) MySqlSchemaParser.db_conn
				.createStatement();}
		int Qsize=DMLQueue.getQueueSize();
		if(Qsize>Stats.maxCombinerToBatchSize)
			Stats.maxCombinerToBatchSize=Qsize;
		else if(Qsize<Stats.minCombinerToBatchSize)
			Stats.minCombinerToBatchSize=Qsize;
		if(Stats.NoDMLsPassedToBatcher.get(Qsize)!=null){
			int val=Stats.NoDMLsPassedToBatcher.get(Qsize);
			Stats.NoDMLsPassedToBatcher.remove(Qsize);
			Stats.NoDMLsPassedToBatcher.put(Qsize, val+1);
		}
		else{
			Stats.NoDMLsPassedToBatcher.put(Qsize, 1);
		}
		
		if (!DMLQueue.IsEmpty()&& currTable==null && currType==null) {
			DML currDML = DMLQueue.RemoveDMLfromHead();
			currType = currDML.type;
			currTable = currDML.table;
			batch(currDML, currType, currTable);
		}
		while (!DMLQueue.IsEmpty()) {
			DML nextDML = DMLQueue.RemoveDMLfromHead();

			batch(nextDML, currType, currTable);
			currType=nextDML.type;
			currTable=nextDML.table;

		}
		if (statement!=null){
			int[] count=null;
			if(Stats.issueToDBMS)
				 count=statement.executeBatch();
			else{
				statement.clearBatch();
			}
			 
			Stats.dbmsAccess++;
			Set countSet = new HashSet(Arrays.asList(count));
			if (countSet.size()>Stats.maxBatched)
				Stats.maxBatched=countSet.size();
			else if(countSet.size()<Stats.minBatched)
				Stats.minBatched=countSet.size();
			Stats.totalBatched+=countSet.size();
			statement=null;
			currType=null;
			currTable=null;
			
		}
	}


	private static boolean checkBatchingRules(DMLType dml1Type,String dml1Table,DMLType dml2Type, String dml2Table) {
		if ((dml1Type == dml2Type) && (dml1Table.equals( dml2Table)))
			return true;
		return false;
	}

//	public static void BatchAndPush(List<DML> listOfAffectedDMLs)
//			throws SQLException {
//		if (statement==null){
//			 statement=(Statement) MySqlSchemaParser.db_conn
//				.createStatement();
//		}
//		if (!listOfAffectedDMLs.isEmpty()&& currTable==null && currType==null) {
//			DML currDML = DMLQueue.RemoveDMLfromHead();
//			currType = currDML.type;
//			currTable = currDML.table;
//			batch(currDML, currType, currTable);
//		}
//		while (!listOfAffectedDMLs.isEmpty()) {
//			DML curr = DMLQueue.getMinAndRemove(listOfAffectedDMLs);
//			DML next = DMLQueue.getMinAndRemove(listOfAffectedDMLs);
//			if (curr != null && next != null) {
//				if (checkBatchingRules(curr, next)) {
//					Combiner.PKValuesMap.remove(curr);
//					Combiner.FKValuesMap.remove(curr);
//					Combiner.PKValuesMap.remove(next);
//					Combiner.FKValuesMap.remove(next);
//					DML batchRs = batch(curr, next);
//					DMLQueue.AddToHead(batchRs);
//				} else {
//					Combiner.PKValuesMap.remove(curr);
//					Combiner.FKValuesMap.remove(curr);
//					pushToDBMS(curr);
//					DMLQueue.AddToHead(next);
//				}
//
//			} else {// next is empty
//				pushToDBMS(curr);
//			}
//		}
//	}

	public static void batch(DML dml1, DMLType type, String table) throws SQLException {
		int[] count=null;
		
		if(checkBatchingRules(dml1.type,dml1.table,type,table)){
			statement.addBatch(dml1.toDMLString());
			batchSize++;
		} else {
			if(Stats.issueToDBMS){
				 count=statement.executeBatch();
				 if (batchSize>Stats.maxBatched)
					 Stats.maxBatched=batchSize;
				 else if (batchSize<Stats.minBatched)
					 Stats.minBatched=batchSize;
				 batchSize=0;
			}
				
			else{
				statement.clearBatch();
			}
			Stats.dbmsAccess++;
			Set countSet = new HashSet(Arrays.asList(count));
			if (countSet.size()>Stats.maxBatched)
				Stats.maxBatched=countSet.size();
			else if(countSet.size()<Stats.minBatched)
				Stats.minBatched=countSet.size();
			Stats.totalBatched+=countSet.size();
			statement.addBatch(dml1.toDMLString());
		}
		
	}
	public static void blindBatch() throws SQLException{
		batchSize=0;
		Stats.batchCalls++;
		Combiner.PKValuesMap.clear();
		if (statement==null){
		statement=  (Statement) MySqlSchemaParser.db_conn
				.createStatement();}
		int Qsize=DMLQueue.getQueueSize();
		if(Qsize>Stats.maxCombinerToBatchSize)
			Stats.maxCombinerToBatchSize=Qsize;
		if(Qsize<Stats.minCombinerToBatchSize)
			Stats.minCombinerToBatchSize=Qsize;
		if(Stats.NoDMLsPassedToBatcher.get(Qsize)!=null){
			int val=Stats.NoDMLsPassedToBatcher.get(Qsize);
			Stats.NoDMLsPassedToBatcher.remove(Qsize);
			Stats.NoDMLsPassedToBatcher.put(Qsize, val+1);
		}
		else{
			Stats.NoDMLsPassedToBatcher.put(Qsize, 1);
		}
		
		while (!DMLQueue.IsEmpty()) {
			String d=DMLQueue.RemoveDMLfromHead().toDMLString();
			statement.addBatch(d.substring(0, d.length()-1));
		}
		if (statement!=null){
			int[] count = null;
			if(Stats.issueToDBMS)
				 count=statement.executeBatch();
			else{
				statement.clearBatch();
			}
			 
			Stats.dbmsAccess++;
			Set countSet = new HashSet(Arrays.asList(count));
			if (countSet.size()>Stats.maxBatched)
				Stats.maxBatched=countSet.size();
			else if(countSet.size()<Stats.minBatched)
				Stats.minBatched=countSet.size();
			Stats.totalBatched+=countSet.size();
			statement=null;
			
		}
	}

	public static void BatchAndPush(List<DML> listOfAffectedDMLs) {
		// TODO Auto-generated method stub for record level fence
		System.out.println("To be implemented");
	}

	}

