package main;

import java.sql.SQLException;
import java.util.PriorityQueue;

import com.mysql.jdbc.Statement;

import model.DML;
import model.DMLQueue;
import util.Stats;

public class BlindBatcher extends Batcher{
	private static Statement statement=null;
	private static DML currDML=null;

	@Override
	public void BatchAndPush(PriorityQueue<DML> affectedDMLs) throws SQLException{
		Combiner.PKValuesMap.clear();
		MySqlSchemaParser.db_conn.setAutoCommit(false);
		if (statement == null) {
			statement = (Statement) MySqlSchemaParser.db_conn.createStatement();
		}

		while (!affectedDMLs.isEmpty()) {
			  currDML = affectedDMLs.remove();
			statement.addBatch(currDML.toDMLString());
		}
		if (statement != null) {
			statement.executeBatch();
			MySqlSchemaParser.db_conn.commit();
			statement.clearBatch();
			statement = null;
			statement.close();
			Stats.dbmsAccess++;

		}
		
	}

	@Override
	public void BatchAndPush() throws SQLException {
		Combiner.PKValuesMap.clear();
		MySqlSchemaParser.db_conn.setAutoCommit(false);
		if (statement == null) {
			statement = (Statement) MySqlSchemaParser.db_conn.createStatement();
		}

		while (!DMLQueue.IsEmpty()) {
			String d = DMLQueue.RemoveDMLfromHead().toDMLString();
			statement.addBatch(d);
		}
		if (statement != null) {
			statement.executeBatch();
			MySqlSchemaParser.db_conn.commit();
			statement.clearBatch();
			statement = null;
			statement.close();
			Stats.dbmsAccess++;

		}
		
	}

	@Override
	public void printStats() {
		// TODO Auto-generated method stub
		
	}

}
