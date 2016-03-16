package util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import main.Combiner;
import main.MySqlSchemaParser;
import model.DML;
import model.DMLQueue;

import com.mysql.jdbc.Statement;

public class Util {

	public static String[] splitDMLsByOR(String dmlString)
	{
		dmlString = dmlString.replace(";", " ");
		dmlString = dmlString.trim();
		String[] parts = dmlString.split(" WHERE ");
		if (parts.length == 1)
			return new String[]{dmlString};
		
		String DMLPre = parts[0];
		String[] DMLPosts = parts[1].split(" OR ");
		
		String NewDMLs[] = new String[DMLPosts.length];
		
		for(int idx = 0; idx < DMLPosts.length; idx++)
		{
			String whereClause = DMLPosts[idx];
			String newDML = DMLPre.trim() + " WHERE " +  whereClause.trim() +";";
			NewDMLs[idx] = newDML; 
		}
		
		return NewDMLs;
	}

	public static void BatchAndPush() throws SQLException {//For TableLevelFence
		// TODO Auto-generated method stub
		Combiner.PKValuesMap.clear();
		Combiner.FKValuesMap.clear();
		DML dml=DMLQueue.getDMLQueueHead();
		while(dml!=null&&dml.NextNode!=null){
			if(checkBatchingRules(dml,dml.NextNode)){
				//Remove dml and next from DMLQUEUE
				DML batchRs=batch(dml,dml.NextNode);
				//Add dml to the head of DMLQueue
				dml=DMLQueue.getDMLQueueHead();
			}
			else{
				//Remove DML from DMLQueue
			String dmlstr=dml.DMLString;
			Statement stmt = (Statement) MySqlSchemaParser.db_conn.createStatement();
			stmt.execute(dmlstr);
			dml=DMLQueue.getDMLQueueHead();
			}
		}
		if(dml!=null){//queueIsEmpty()
			//Remove DML from DMLQueue
			String dmlstr=dml.DMLString;
			Statement stmt = (Statement) MySqlSchemaParser.db_conn.createStatement();
			stmt.execute(dmlstr);
		}
		
		
	}

	private static boolean checkBatchingRules(DML dml, DML nextNode) {
		// TODO Auto-generated method stub
		return false;
	}

	public static void BatchAndPush(List<DML> listOfAffectedDMLs) {
		// TODO Auto-generated method stub
		
	}
	public static DML batch(DML dml1,DML dml2){
		return dml2;
		
	}
}
