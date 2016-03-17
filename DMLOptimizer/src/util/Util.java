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
		String[] parts = dmlString.split(" where ");
		if (parts.length == 1)
			return new String[]{dmlString};
		
		String DMLPre = parts[0];
		String[] DMLPosts = parts[1].split(" or ");
		
		String NewDMLs[] = new String[DMLPosts.length];
		
		for(int idx = 0; idx < DMLPosts.length; idx++)
		{
			String whereClause = DMLPosts[idx];
			String newDML = DMLPre.trim() + " where " +  whereClause.trim() +";";
			NewDMLs[idx] = newDML; 
		}
		
		return NewDMLs;
	}

	public static void BatchAndPush() throws SQLException {//For TableLevelFence
		// TODO Auto-generated method stub
		Combiner.PKValuesMap.clear();
		Combiner.FKValuesMap.clear();
		DML currDML=DMLQueue.getDMLQueueHead();
		while(!DMLQueue.IsEmpty()&&currDML.NextNode!=null){
			if(checkBatchingRules(currDML,currDML.NextNode)){
				//Remove dml and next from DMLQUEUE
				currDML=DMLQueue.RemoveDMLfromHead();
				DML nextDML=DMLQueue.RemoveDMLfromHead();
				DML batchRs=batch(currDML,nextDML);
				//Add dml to the head of DMLQueue
				DMLQueue.AddToHead(batchRs);
				currDML=DMLQueue.getDMLQueueHead();
			}
			else{
				//Remove DML from DMLQueue
			currDML=DMLQueue.RemoveDMLfromHead();
			String dmlstr=currDML.DMLString;
			Statement stmt = (Statement) MySqlSchemaParser.db_conn.createStatement();
			stmt.execute(dmlstr);
			currDML=DMLQueue.getDMLQueueHead();
			}
		}
		if(!DMLQueue.IsEmpty()){//queueIsEmpty()
			//Remove DML from DMLQueue
			currDML=DMLQueue.RemoveDMLfromHead();
			String dmlstr=currDML.DMLString;
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
