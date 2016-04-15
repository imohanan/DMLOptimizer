package util;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import com.mysql.jdbc.Statement;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

import main.Combiner;
import main.MySqlSchemaParser;
import model.DML;
import model.DMLQueue;
import model.DMLType;

public class Util {
	private static Statement statement = null;
	private static String template = null;
	private static java.sql.PreparedStatement preparedStatement = null;
	private static DMLType currType = null;
	private static DML currDML = null;
	private static String currTable = null;
	public static int batchSize;

	public static String preprocessDMLString(String dmlLine) {
		dmlLine = dmlLine.replace("=", " = ");
		dmlLine = dmlLine.replace("(", " (");
		dmlLine = dmlLine.replace(")", ") ");
		dmlLine = dmlLine.replaceAll("( )+", " ");
		dmlLine = dmlLine.replaceAll("\\s+(?=[^()]*\\))", "");
		return dmlLine;
	}

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
			String newDML = DMLPre.trim() + " where " + whereClause.trim() + ";";
			NewDMLs[idx] = newDML;
		}

		return NewDMLs;
	}

	public static void ManualBatchAndPush() throws SQLException {
		Stats.batchCalls++;
		if (Stats.minCombinerToBatchSize > DMLQueue.queueSize)
			Stats.minCombinerToBatchSize = DMLQueue.queueSize;
		if (Stats.maxCombinerToBatchSize < DMLQueue.queueSize)
			Stats.maxCombinerToBatchSize = DMLQueue.queueSize;

		Statement manualStatement = (Statement) MySqlSchemaParser.db_conn.createStatement();
		List<DML> DMLsToBatch = new LinkedList<DML>();

		while (DMLQueue.IsEmpty() == false) {
			DML currDML = DMLQueue.RemoveDMLfromHead();

			if ((DMLsToBatch.isEmpty() == true || checkBatchingRules(currDML.type, currDML.table,
					DMLsToBatch.get(0).type, DMLsToBatch.get(0).table) == true) && currDML.IsRecordLevelFence == false
					&& currDML.IsTableLevelFence == false && currDML.type != DMLType.UPDATE) // Not
																								// attempting
																								// to
																								// batch
																								// update
																								// DMLs
			{
				DMLsToBatch.add(currDML);
			} else {
				if (DMLsToBatch.isEmpty() == false) {
					String batchedStatement = getBatchedStatement(DMLsToBatch);
					manualStatement.addBatch(batchedStatement);
				}

				DMLsToBatch = new LinkedList<DML>();
				DMLsToBatch.add(currDML);
			}
		}

		if (!DMLsToBatch.isEmpty()) {
			String batchedStatement = getBatchedStatement(DMLsToBatch);
			manualStatement.addBatch(batchedStatement);
		}

		int[] results = manualStatement.executeBatch();
		manualStatement.clearBatch();
		manualStatement.close();
		Stats.dbmsAccess++;
	}

	public static void ManualBatchAndPush(PriorityQueue<DML> affectedDMLs) throws SQLException {
		Stats.batchCalls++;
		if (Stats.minCombinerToBatchSize > affectedDMLs.size())
			Stats.minCombinerToBatchSize = affectedDMLs.size();
		if (Stats.maxCombinerToBatchSize < affectedDMLs.size())
			Stats.maxCombinerToBatchSize = affectedDMLs.size();

		Statement manualStatement = (Statement) MySqlSchemaParser.db_conn.createStatement();
		List<DML> DMLsToBatch = new LinkedList<DML>();

		while (affectedDMLs.isEmpty() == false) {
			DML currDML = affectedDMLs.remove();

			if ((DMLsToBatch.isEmpty() == true || checkBatchingRules(currDML.type, currDML.table,
					DMLsToBatch.get(0).type, DMLsToBatch.get(0).table) == true) && currDML.IsRecordLevelFence == false
					&& currDML.IsTableLevelFence == false && currDML.type != DMLType.UPDATE) // Not
																								// attempting
																								// to
																								// batch
																								// update
																								// DMLs
			{
				DMLsToBatch.add(currDML);
			} else {
				if (DMLsToBatch.isEmpty() == false) {
					String batchedStatement = getBatchedStatement(DMLsToBatch);
					manualStatement.addBatch(batchedStatement);
				}
				DMLsToBatch = new LinkedList<DML>();
				DMLsToBatch.add(currDML);
			}
		}

		if (DMLsToBatch.isEmpty() == false) {
			String batchedStatement = getBatchedStatement(DMLsToBatch);
			manualStatement.addBatch(batchedStatement);
		}

		int[] results = manualStatement.executeBatch();
		manualStatement.clearBatch();
		manualStatement.close();
		Stats.dbmsAccess++;
	}

	private static String getBatchedStatement(List<DML> DMLsToBatch) {
		Stats.countManualBatcher++;
		if (DMLsToBatch.size() > Stats.maxBatched)
			Stats.maxBatched = DMLsToBatch.size();
		if (DMLsToBatch.size() < Stats.minBatched)
			Stats.minBatched = DMLsToBatch.size();

		if (DMLsToBatch.size() == 1)
			return DMLsToBatch.get(0).DMLString;
		if (DMLsToBatch.get(0).type == DMLType.INSERT) {
			String batchedStatement = ManualBatching.batchInsert(DMLsToBatch);
			return batchedStatement;
		} else if (DMLsToBatch.get(0).type == DMLType.DELETE) {
			String batchedStatement = ManualBatching.batchDelete(DMLsToBatch);
			return batchedStatement;
		}
		return null;
	}

	public static void BatchAndPush() throws SQLException {// For
		// TableLevelFence
		batchSize = 0;
		Stats.batchCalls++;
		Combiner.PKValuesMap.clear();
		if (statement == null) {
			statement = (Statement) MySqlSchemaParser.db_conn.createStatement();
		}
		int Qsize = DMLQueue.getQueueSize();
		if (Qsize > Stats.maxCombinerToBatchSize)
			Stats.maxCombinerToBatchSize = Qsize;
		else if (Qsize < Stats.minCombinerToBatchSize)
			Stats.minCombinerToBatchSize = Qsize;
		if (Stats.NoDMLsPassedToBatcher.get(Qsize) != null) {
			int val = Stats.NoDMLsPassedToBatcher.get(Qsize);
			Stats.NoDMLsPassedToBatcher.remove(Qsize);
			Stats.NoDMLsPassedToBatcher.put(Qsize, val + 1);
		} else {
			Stats.NoDMLsPassedToBatcher.put(Qsize, 1);
		}

		if (!DMLQueue.IsEmpty() && currTable == null && currType == null) {
			DML currDML = DMLQueue.RemoveDMLfromHead();
			currType = currDML.type;
			currTable = currDML.table;
			batch(currDML, currType, currTable);
		}
		while (!DMLQueue.IsEmpty()) {
			DML nextDML = DMLQueue.RemoveDMLfromHead();
			batch(nextDML, currType, currTable);
			currType = nextDML.type;
			currTable = nextDML.table;

		}
		if (statement != null) {
			int[] count = null;
			if (Stats.issueToDBMS)
				count = statement.executeBatch();
			else {
				statement.clearBatch();
			}

			Stats.dbmsAccess++;
			Set countSet = new HashSet(Arrays.asList(count));
			if (countSet.size() > Stats.maxBatched)
				Stats.maxBatched = countSet.size();
			else if (countSet.size() < Stats.minBatched)
				Stats.minBatched = countSet.size();
			Stats.totalBatched += countSet.size();
			statement = null;
			currType = null;
			currTable = null;

		}
	}

	private static boolean checkBatchingRules(DMLType dml1Type, String dml1Table, DMLType dml2Type, String dml2Table) {
		if ((dml1Type == dml2Type) && (dml1Table.equals(dml2Table)))
			return true;
		return false;
	}

	public static void batch(DML dml1, DMLType type, String table) throws SQLException {

		if (checkBatchingRules(dml1.type, dml1.table, type, table)) {
			statement.addBatch(dml1.toDMLString());
			batchSize++;
		} else {
				statement.executeBatch();
				statement.clearBatch();
			}
			Stats.dbmsAccess++;
			statement.addBatch(dml1.toDMLString());
		}


	public static void blindBatch() throws SQLException {
		batchSize = 0;
		Stats.batchCalls++;
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
	public static void blindBatchRLF(PriorityQueue<DML> affectedDMLs) throws SQLException {
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


	public static void batchUsingPreparedStatementRLF(PriorityQueue<DML> affectedDMLs) throws SQLException {
		batchSize = 0;
		Stats.batchCalls++;
		MySqlSchemaParser.db_conn.setAutoCommit(false);
		int sizeofheap = affectedDMLs.size();
		// Stats.DMLSentToBatcher += sizeofheap;
		if (sizeofheap > Stats.maxCombinerToBatchSize)
			Stats.maxCombinerToBatchSize = sizeofheap;
		if (sizeofheap < Stats.minCombinerToBatchSize)
			Stats.minCombinerToBatchSize = sizeofheap;
		int batchcount = 0;
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
					Stats.dbmsAccess++;
					if (Stats.minBatched > 1)
						Stats.minBatched = 1;
					if (Stats.maxBatched < 1)
						Stats.maxBatched = 1;
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

	public static void batchUsingPreparedStatement() throws SQLException {
		batchSize = 0;
		Stats.batchCalls++;
		int sizeofqueue = DMLQueue.getQueueSize();
		// Stats.DMLSentToBatcher += sizeofqueue;
		if (sizeofqueue > Stats.maxCombinerToBatchSize)
			Stats.maxCombinerToBatchSize = sizeofqueue;
		if (sizeofqueue < Stats.minCombinerToBatchSize)
			Stats.minCombinerToBatchSize = sizeofqueue;
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
					Stats.dbmsAccess++;
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

	public static void executePreparedStatement() throws SQLException {
		int counts[] = preparedStatement.executeBatch();
		int batchsize = counts.length;
		if (batchsize > Stats.maxBatched)
			Stats.maxBatched = batchsize;
		if (batchsize < Stats.minBatched)
			Stats.minBatched = batchsize;
		preparedStatement.clearBatch();
		MySqlSchemaParser.db_conn.commit();
		Stats.dbmsAccess++;
		preparedStatement = null;
		currTable = null;
		currType = null;
	}

	public static void fillPreparedStatement(String table, DML dml) throws SQLException {
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

			if (attrVal.equalsIgnoreCase("null") && !attrType.equalsIgnoreCase("array")
					&& !attrType.equalsIgnoreCase("BLOB") && !attrType.equalsIgnoreCase("CLOB")
					&& !attrType.equalsIgnoreCase("DATALINK") && !attrType.equalsIgnoreCase("JAVA_OBJECT")
					&& !attrType.equalsIgnoreCase("NCHAR") && !attrType.equalsIgnoreCase("NCLOB")
					&& !attrType.equalsIgnoreCase("NVARCHAR") && !attrType.equalsIgnoreCase("LONGNVARCHAR")
					&& !attrType.equalsIgnoreCase("REF") && !attrType.equalsIgnoreCase("ROWID")
					&& !attrType.equalsIgnoreCase("SQLXML") && !attrType.equalsIgnoreCase("STRUCT")) {
				if (attrType.equalsIgnoreCase("VARCHAR"))
					preparedStatement.setNull(attrCount, java.sql.Types.VARCHAR);
				else if (attrType.equalsIgnoreCase("int"))
					preparedStatement.setNull(attrCount, java.sql.Types.INTEGER);
				else if (attrType.equalsIgnoreCase("double"))
					preparedStatement.setNull(attrCount, java.sql.Types.DOUBLE);
				else if (attrType.equalsIgnoreCase("boolean"))
					preparedStatement.setNull(attrCount, java.sql.Types.BOOLEAN);
				else if (attrType.equalsIgnoreCase("float"))
					preparedStatement.setNull(attrCount, java.sql.Types.FLOAT);
				else if (attrType.equalsIgnoreCase("SMALLINT "))
					preparedStatement.setNull(attrCount, java.sql.Types.SMALLINT);
				else if (attrType.equalsIgnoreCase("TINYINT"))
					preparedStatement.setLong(attrCount, java.sql.Types.TINYINT);
				else if (attrType.equalsIgnoreCase("BIGINT"))
					preparedStatement.setLong(attrCount, java.sql.Types.BIGINT);
				else if (attrType.equalsIgnoreCase("DECIMAL"))
					preparedStatement.setLong(attrCount, java.sql.Types.DECIMAL);
				else if (attrType.equalsIgnoreCase("char"))
					preparedStatement.setLong(attrCount, java.sql.Types.CHAR);

			} else {
				if (attrType.equalsIgnoreCase("VARCHAR") || attrType.equalsIgnoreCase("LONGVARCHAR"))
					preparedStatement.setString(attrCount, attrVal);
				else if (attrType.equalsIgnoreCase("int"))
					preparedStatement.setInt(attrCount, Integer.parseInt(attrVal));
				else if (attrType.equalsIgnoreCase("double"))
					preparedStatement.setDouble(attrCount, Double.parseDouble(attrVal));
				else if (attrType.equalsIgnoreCase("boolean"))
					preparedStatement.setBoolean(attrCount, Boolean.parseBoolean(attrVal));
				else if (attrType.equalsIgnoreCase("float") || attrType.equalsIgnoreCase("DECIMAL"))
					preparedStatement.setFloat(attrCount, Float.parseFloat(attrVal));
				else if (attrType.equalsIgnoreCase("long") || attrType.equalsIgnoreCase("BIGINT"))
					preparedStatement.setLong(attrCount, Long.parseLong(attrVal));
				else if (attrType.equalsIgnoreCase("short") || attrType.equalsIgnoreCase("TINYINT")
						|| attrType.equalsIgnoreCase("SMALLINT"))
					preparedStatement.setShort(attrCount, Short.parseShort(attrVal));
				else if (attrType.equalsIgnoreCase("string") || attrType.equalsIgnoreCase("char"))
					preparedStatement.setString(attrCount, attrVal);
				else if (attrType.equalsIgnoreCase("byte"))
					preparedStatement.setByte(attrCount, Byte.parseByte(attrVal));
				else if (attrType.equalsIgnoreCase("nstring"))
					preparedStatement.setNString(attrCount, attrVal);

			}

			attrCount++;
		}
	}

}
