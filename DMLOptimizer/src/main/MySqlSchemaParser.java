package main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.sound.midi.SysexMessage;

import model.Fkey;

public class MySqlSchemaParser {
	public static boolean verbose = false;
	public static Map<String, List<String>> TableAttrs = new HashMap<String, List<String>>();
	public static Map<String,Map<String,String>>AttrTypes= new HashMap<String,Map<String,String>>();
	public static Map<String, List<String>> TablePkeys = new HashMap<String, List<String>>();
	public static Map<String, List<Fkey>> TableFkeys = new HashMap<String, List<Fkey>>();
	public static Map<String, Set<String>> ImpactedTables = new HashMap<String, Set<String>>();
	public static Map<String,Map<String,String>> AttrInitVal=new HashMap<String,Map<String,String>>();
	public static List<String> TablesInDB = new LinkedList<String>();
	public static Connection db_conn = null;

	public static void init_Schema(String username,String password, String db) throws SQLException {
		setupConnection( username, password, db);
		
		TablesInDB=getAllTables(db);
		for (String table: TablesInDB){
			TableAttrs.put(table,getAttributes(table));
			getAttrTypes(table);
			getAttrDefaultValue(table);
			TablePkeys.put(table, getPkey(table));
			TableFkeys.put(table, getFKey(table));
		}
		fillImpactedTables();
		if (verbose)printImpactedTables();	
	}

	public static void getAttrTypes(String table) throws SQLException{//TODO:Check if it works fine.--Shiva--
		Map<String,String> attrToTypeList=new HashMap<String,String>();
		ResultSet rsColumns = null;
		java.sql.DatabaseMetaData metadata = db_conn.getMetaData();
	    rsColumns = metadata.getColumns(null, null, table, null);
	    while (rsColumns.next()) {
	    	String type=rsColumns.getString("TYPE_NAME").toLowerCase();
	    	String col=rsColumns.getString("COLUMN_NAME").toLowerCase();
	    	attrToTypeList.put(col,type);
	    }
	    AttrTypes.put(table, attrToTypeList);
	}
	public static void getAttrDefaultValue(String table) throws SQLException{
		Map<String,String> attrToDefVal=new HashMap<String,String>();
		ResultSet rsColumns = null;
		java.sql.DatabaseMetaData metadata = db_conn.getMetaData();
	    rsColumns = metadata.getColumns(null, null, table, null);
	    while (rsColumns.next()) {
	    	String defVal= rsColumns.getString("COLUMN_DEF");//Do not change it to lowercase.
	    	String col=rsColumns.getString("COLUMN_NAME").toLowerCase();
	    	attrToDefVal.put(col,defVal);
	   
	    }
	    AttrInitVal.put(table, attrToDefVal);
	}
	public static void setupConnection(String username,String password, String db) {

		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("No MySQL JDBC Driver Found.");
			e.printStackTrace();
			return;
		}

		System.out.println("MySQL JDBC Driver Registered!");

		try {
			db_conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/", username, password);

		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}

		if (db_conn != null) {
			try {
				if(doesDBExist(db)){
					db_conn.close();
					db_conn = DriverManager.getConnection(
							"jdbc:mysql://localhost:3306/"+db+
							        "?rewriteBatchedStatements=true", username, password);
				System.out.println("You made it, take control your database now!");
				}
				else{System.out.println("Failed to make connection! Check if database "+db+" exists.");
				System.exit(-1);
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("Failed to make connection! Check if database "+db+" exists.");
				System.exit(-1);
			}
		} else {
			System.out.println("Failed to make connection!");
			System.exit(-1);
		}

	}

	public static String getAttrType(String tableName, String attrname) {
		String result = "";
		Statement stmt = null;
		ResultSet rs = null;
		String qry = "SELECT data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"
				+ tableName + "' and column_name='" + attrname + "';";
		if (!doesTableExist(tableName)) {
			System.out.println("Table" + tableName + " Does Not Exist.");
			return null;
		}
		try {
			System.out.println(qry);
			stmt = db_conn.createStatement();
			rs = stmt.executeQuery(qry);
			while (rs.next()) {
				result += rs.getString("data_type").toLowerCase();
			}

		} catch (SQLException ex) {
			System.out.println("SQLException: " + ex.getMessage());
		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException sqlEx) {
				}

				rs = null;
			}

			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException sqlEx) {
				}

				stmt = null;
			}

		}
		return result;
	}

	public static List<String> getAllTables(String dbName) {
		List<String> result = new LinkedList<String>();
		Statement stmt = null;
		ResultSet rs = null;
		String qry = "show tables from " + dbName;
		try {
			if (verbose)System.out.println(qry);
			stmt = db_conn.createStatement();
			rs = stmt.executeQuery(qry);
			while (rs.next()) {
				result.add(rs.getString("Tables_in_" + dbName).toLowerCase());
				if (verbose)
					System.out.println(rs.getString("Tables_in_" + dbName).toLowerCase());
			}

		} catch (SQLException ex) {
			System.out.println("SQLException: " + ex.getMessage());
		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException sqlEx) {
				}

				rs = null;
			}

			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException sqlEx) {
				}

				stmt = null;
			}

		}
		return result;
	}

	public static List<String> getAttributes(String tableName) {
		List<String> result = new LinkedList<String>();
		ResultSet rs = null;
		if (!doesTableExist(tableName)) {
			System.out.println("Table" + tableName + " Does Not Exist.");
			return null;
		}
		try {
			java.sql.DatabaseMetaData metadata = db_conn.getMetaData();
			rs = metadata.getColumns(null, null, tableName, null);
			while (rs.next()) {
				result.add(rs.getString("COLUMN_NAME").toLowerCase());
				if (verbose)
					System.out.println(rs.getString("COLUMN_NAME").toLowerCase());
			}

		} catch (SQLException ex) {
			System.out.println("SQLException: " + ex.getMessage());
		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException sqlEx) {
				}

				rs = null;
			}

		}
		return result;

	}

	public static List<String> getPkey(String tableName) {
		List<String> result = new LinkedList<String>();
		ResultSet rs = null;
		if (!doesTableExist(tableName)) {
			System.out.println("Table" + tableName + " Does Not Exist.");
			return null;
		}
		try {
			java.sql.DatabaseMetaData metadata = db_conn.getMetaData();
			rs = metadata.getPrimaryKeys(null, null, tableName);
			while (rs.next()) {
				result.add(rs.getString("COLUMN_NAME").toLowerCase());
				if (verbose)
					System.out.println(rs.getString("COLUMN_NAME").toLowerCase());
			}

		} catch (SQLException ex) {
			System.out.println("SQLException: " + ex.getMessage());
		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException sqlEx) {
				}

				rs = null;
			}

		}
//		if (result.isEmpty()) {
//			System.out.println("No Primary Key Found For " + tableName);
//		}
		return result;

	}

	public static boolean doesTableExist(String tableName) {
		List<String> qryResult = new LinkedList<String>();
		ResultSet rs = null;
		try {
			java.sql.DatabaseMetaData metadata = db_conn.getMetaData();
			rs = metadata.getTables(null, null, tableName, null);
			while (rs.next()) {
				qryResult.add(rs.getString("TABLE_NAME").toLowerCase());
				if (verbose)
					System.out.println(rs.getString("TABLE_NAME").toLowerCase());
			}

		} catch (SQLException ex) {
			System.out.println("SQLException: " + ex.getMessage());
		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException sqlEx) {
				}

				rs = null;
			}

		}
		if (qryResult.isEmpty()) {
			return false;
		}

		else
			return true;
	}

	public static List<Fkey> getFKey(String tableName) {
		List<Fkey> fkeys = new LinkedList<Fkey>();
		Map<String, Fkey> fkMap = new HashMap<String, Fkey>();
		if (!doesTableExist(tableName)) {
			System.out.println("Table" + tableName + " Does Not Exist.");
			return null;
		}
		ResultSet rs = null;
		try {
			java.sql.DatabaseMetaData metadata = db_conn.getMetaData();
			rs = metadata
					.getImportedKeys(db_conn.getCatalog(), null, tableName);
			while (rs.next()) {
				Fkey currentFK;
				List<String> pk_cols = new LinkedList<String>();
				List<String> fk_cols = new LinkedList<String>();
				currentFK = fkMap.get(rs.getString("FK_NAME").toLowerCase());
				if (currentFK != null) {
					currentFK.addFk_cols(rs.getString("FKCOLUMN_NAME").toLowerCase());
					currentFK.addPk_cols(rs.getString("PKCOLUMN_NAME").toLowerCase());
				} else {
					pk_cols.add(rs.getString("PKCOLUMN_NAME").toLowerCase());
					fk_cols.add(rs.getString("FKCOLUMN_NAME").toLowerCase());
					currentFK = new Fkey(rs.getString("FK_NAME").toLowerCase(),
							rs.getString("PKTABLE_NAME").toLowerCase(),
							rs.getString("FKTABLE_NAME").toLowerCase(), pk_cols, fk_cols,
							rs.getString("DELETE_RULE").toLowerCase());
					fkMap.put(rs.getString("FK_NAME").toLowerCase(), currentFK);
					fkeys.add(currentFK);
				}

			}

		} catch (SQLException ex) {
			System.out.println("SQLException: " + ex.getMessage());
		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException sqlEx) {
				}

				rs = null;
			}

		}

		return fkeys;
	}
	
	
	public static void fillImpactedTables(){
		Iterator<Entry<String, List<Fkey>>> it = TableFkeys.entrySet().iterator();
		 while (it.hasNext()) {
			 List<Fkey> fkeys=new LinkedList<Fkey>();
		        Map.Entry<String,List<Fkey>> pair = (Map.Entry<String,List<Fkey>>)it.next();
		        fkeys=(List<Fkey>) pair.getValue();
		        for(Fkey fk:fkeys){
		        	if(ImpactedTables.get(fk.getPk_table())==null){
		        		Set<String> fk_tbl=new HashSet<String>();
		        		fk_tbl.add(fk.getFk_table());
		        		ImpactedTables.put(fk.getPk_table(),fk_tbl);
		        	}
		        	else{
		        		Set<String> fklist=ImpactedTables.get(fk.getPk_table());
		        		fklist.add(fk.getFk_table());
		        		ImpactedTables.remove(fk.getPk_table());
		        		ImpactedTables.put(fk.getPk_table(),fklist);
		        	}
		        }
		    }
	}
	public static void printImpactedTables(){
		Iterator<Entry<String, Set<String>>> it=ImpactedTables.entrySet().iterator();
		while(it.hasNext()){
			Set<String> impacteds=new HashSet<String>();
			Map.Entry<String, Set<String>> pair=(Map.Entry<String,Set<String>>)it.next();
			impacteds=pair.getValue();
			System.out.print("Primary Table: "+pair.getKey()+" Foreign Tables: ");
			for(String table:impacteds){
				System.out.print(" "+table+",");
			}
			System.out.println("\n");
			
		}
	}
	
	public static boolean doesDBExist(String db) throws SQLException{
		ResultSet rs=db_conn.getMetaData().getCatalogs();
		while (rs.next()){
			if (rs.getString(1).equalsIgnoreCase(db))
				return true;
		}
		return false;
	}
}

