package util;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import main.Combiner;
import main.MySqlSchemaParser;
import model.DML;
import model.DMLQueue;
import model.DMLType;

import com.mysql.jdbc.Statement;

public class Util {

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
		// TODO Auto-generated method stub
		Combiner.PKValuesMap.clear();
		Combiner.FKValuesMap.clear();
		DML currDML = DMLQueue.getDMLQueueHead();
		while (!DMLQueue.IsEmpty() && currDML.NextNode != null) {
			if (checkBatchingRules(currDML, currDML.NextNode)) {
				currDML = DMLQueue.RemoveDMLfromHead();
				DML nextDML = DMLQueue.RemoveDMLfromHead();
				DML batchRs = batch(currDML, nextDML);
				DMLQueue.AddToHead(batchRs);
				currDML = DMLQueue.getDMLQueueHead();
			} else {
				currDML = DMLQueue.RemoveDMLfromHead();
				pushToDBMS(currDML);
			}
		}
		if (!DMLQueue.IsEmpty()) {
			currDML = DMLQueue.RemoveDMLfromHead();
			pushToDBMS(currDML);
		}

	}

	public static void pushToDBMS(DML currDML) throws SQLException {
		String dmlstr = currDML.DMLString;
		Statement stmt = (Statement) MySqlSchemaParser.db_conn
				.createStatement();
		stmt.execute(dmlstr);
	}

	private static boolean checkBatchingRules(DML dml1, DML dml2) {
		if ((dml1.type == DMLType.INSERT || dml1.type == DMLType.UPDATE)
				&& (dml1.type == dml2.type) && (dml1.table == dml2.table)) {
			return true;
		}
		return false;
	}

	public static void BatchAndPush(List<DML> listOfAffectedDMLs)
			throws SQLException {
		while (!listOfAffectedDMLs.isEmpty()) {
			DML curr = DMLQueue.getMinAndRemove(listOfAffectedDMLs);
			DML next = DMLQueue.getMinAndRemove(listOfAffectedDMLs);
			if (curr != null && next != null) {
				if (checkBatchingRules(curr, next)) {
					Combiner.PKValuesMap.remove(curr);
					Combiner.FKValuesMap.remove(curr);
					Combiner.PKValuesMap.remove(next);
					Combiner.FKValuesMap.remove(next);
					DML batchRs = batch(curr, next);
					DMLQueue.AddToHead(batchRs);
				} else {
					Combiner.PKValuesMap.remove(curr);
					Combiner.FKValuesMap.remove(curr);
					pushToDBMS(curr);
					DMLQueue.AddToHead(next);
				}

			} else {// next is empty
				pushToDBMS(curr);
			}
		}
	}

	public static DML batch(DML dml1, DML dml2) {
//		DML batched=null;
//		if((dml1.type==DMLType.INSERT) && (dml2.type==DMLType.INSERT)){
//			batched.type=DMLType.INSERT;
//			batched.table=dml1.table;
//			Map<String,String> dml1Vals=dml1.DMLGetAttributeValues;
//			Map<String,String> dml2Vals=dml2.DMLGetAttributeValues;
//			for ()
//		}
		return dml2;

	}
}
