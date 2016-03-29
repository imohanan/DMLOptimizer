package util;

import java.sql.SQLException;
import java.util.List;

import main.Combiner;
import main.MySqlSchemaParser;
import model.DML;
import model.DMLQueue;
import model.DMLType;

import com.mysql.jdbc.Statement;

public class Util {
	private static Statement statement=null;
	private static DMLType currType = null;
	private static String currTable = null;

	public static String[] splitDMLsByOR(String dmlString) {
		dmlString = dmlString.replace(";", " ");
		dmlString = dmlString.trim();
		String[] parts = dmlString.split(" where ");
		if (parts.length == 1)
			return new String[] { dmlString };

		String DMLPre = parts[0];
		String[] DMLPosts = parts[1].split(" or ");

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
		Combiner.PKValuesMap.clear();
		if (statement==null){
		statement=  (Statement) MySqlSchemaParser.db_conn
				.createStatement();}
		if (!DMLQueue.IsEmpty()&& currTable==null && currType==null) {
			DML currDML = DMLQueue.RemoveDMLfromHead();
			currType = currDML.type;
			currTable = currDML.table;
			batch(currDML, currType, currTable);
		}
		while (!DMLQueue.IsEmpty()) {
			DML nextDML = DMLQueue.RemoveDMLfromHead();

			batch(nextDML, currType, currTable);

		}

	}


	private static boolean checkBatchingRules(DMLType dml1Type,String dml1Table,DMLType dml2Type, String dml2Table) {
		if ((dml1Type == DMLType.INSERT || dml1Type == DMLType.UPDATE)
				&& (dml1Type == dml2Type) && (dml1Table == dml2Table)) {
			return true;
		}
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

		if(checkBatchingRules(dml1.type,dml1.table,type,table)){
			statement.addBatch(dml1.toString());
		} else {
			statement.executeBatch();
			statement.addBatch(dml1.toString());
		}

	}
}
