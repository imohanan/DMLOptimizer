package main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import model.Fkey;

public class MySqlSchemaParser {
	public static boolean verbose = false;
	public static Map<String, List<String>> TableAttrs = new HashMap();
	public static Map<String, List<String>> TablePkeys = new HashMap();
	public static Map<String, List<Fkey>> TableFkeys = new HashMap();
	public static Map<String, List<String>> ImpactedTables = new HashMap();
	public static List<String> TablesInDB = new LinkedList<String>();
	public static Connection db_conn = null;

	public static void init_Schema(String db) {
		setupConnection(db);
		TablesInDB=getAllTables(db);
		for (String table: TablesInDB){
			TableAttrs.put(table,getAttributes(table));
			TablePkeys.put(table, getPkey(table));
			TableFkeys.put(table, getFKey(table));
		}
	}

	public static void setupConnection(String db) {

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
					"jdbc:mysql://localhost:3306/" + db, "root", "password");

		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}

		if (db_conn != null) {
			System.out.println("You made it, take control your database now!");
		} else {
			System.out.println("Failed to make connection!");
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
				result += rs.getString("data_type");
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
			System.out.println(qry);
			stmt = db_conn.createStatement();
			rs = stmt.executeQuery(qry);
			while (rs.next()) {
				result.add(rs.getString("Tables_in_" + dbName));
				if (verbose)
					System.out.println(rs.getString("Tables_in_" + dbName));
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
				result.add(rs.getString("COLUMN_NAME"));
				if (verbose)
					System.out.println(rs.getString("COLUMN_NAME"));
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
				result.add(rs.getString("COLUMN_NAME"));
				if (verbose)
					System.out.println(rs.getString("COLUMN_NAME"));
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
		if (result.isEmpty()) {
			System.out.println("No Primary Key Founded For " + tableName);
		}
		return result;

	}

	public static boolean doesTableExist(String tableName) {
		List<String> qryResult = new LinkedList<String>();
		ResultSet rs = null;
		try {
			java.sql.DatabaseMetaData metadata = db_conn.getMetaData();
			rs = metadata.getTables(null, null, tableName, null);
			while (rs.next()) {
				qryResult.add(rs.getString("TABLE_NAME"));
				if (verbose)
					System.out.println(rs.getString("TABLE_NAME"));
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
				System.out.println(rs.getString("FK_NAME"));
				currentFK = fkMap.get(rs.getString("FK_NAME"));
				if (currentFK != null) {
					currentFK.addFk_cols(rs.getString("FKCOLUMN_NAME"));
					currentFK.addPk_cols(rs.getString("PKCOLUMN_NAME"));
				} else {
					pk_cols.add(rs.getString("PKCOLUMN_NAME"));
					fk_cols.add(rs.getString("FKCOLUMN_NAME"));
					currentFK = new Fkey(rs.getString("FK_NAME"),
							rs.getString("PKTABLE_NAME"),
							rs.getString("FKTABLE_NAME"), pk_cols, fk_cols,
							rs.getString("DELETE_RULE"));
					fkMap.put(rs.getString("FK_NAME"), currentFK);
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
	
	public static List<String> getImpactedTables(String tableName){
		List<String> result=new LinkedList<String>();
		Iterator it = TableFkeys.entrySet().iterator();
		 while (it.hasNext()) {
			 List<Fkey> fkeys=new LinkedList<Fkey>();
		        Map.Entry pair = (Map.Entry)it.next();
		        fkeys=(List<Fkey>) pair.getValue();
		        for(Fkey fk:fkeys){
		        	if(ImpactedTables.get(fk.getPk_table())!=null){
		        		
		        	}
		        }
		    }
		return result;
	}
}