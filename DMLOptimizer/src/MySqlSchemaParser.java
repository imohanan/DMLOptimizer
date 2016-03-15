import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;




public class MySqlSchemaParser {
	private static boolean verbose = false;
	private static HashMap<String, Vector<String>> TableAttrs = new HashMap();
//	private static HashMap<String, Integer> AttrType = new HashMap();
//	private static HashMap<String, String> AttrTypeString = new HashMap();
	private static HashMap<String, Vector<String>> TablePkeys = new HashMap();
	private static HashMap<String, Vector<Fkey>> TableFkeys = new HashMap();
	private static HashMap<String, String> ImpactedTables = new HashMap();
	private static HashMap<String, String> TablesInDB = new HashMap();
	private static Connection db_conn = null;

	public static void init_Schema(String db){
		
		
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
					"jdbc:mysql://localhost:3306/"+db, "root", "password");

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
			System.out.println("Table"+ tableName+" Does Not Exist.");
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
			System.out.println("Table"+ tableName+" Does Not Exist.");
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
			System.out.println("Table"+ tableName+" Does Not Exist.");
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

	public static List<Fkey> getFKey(String tableName){
		List<Fkey> fkeys=new LinkedList<Fkey>();
		if (!doesTableExist(tableName)) {
			System.out.println("Table"+ tableName+" Does Not Exist.");
			return null;
		}
		ResultSet rs = null;
		try {
			java.sql.DatabaseMetaData metadata = db_conn.getMetaData();
			rs = metadata.getImportedKeys(db_conn.getCatalog(), null, tableName);
	while (rs.next()) {
					List<String> pk_cols=new LinkedList<String>();
					List<String> fk_cols=new LinkedList<String>();
					System.out.println(rs.getString("PKTABLE_NAME"));
					System.out.println(rs.getString("PKCOLUMN_NAME"));
					System.out.println(rs.getString("FKTABLE_NAME"));
					System.out.println(rs.getString("FKCOLUMN_NAME"));
					System.out.println(rs.getString("DELETE_RULE"));
					System.out.println("---------------------------");
					pk_cols.add(rs.getString("PKCOLUMN_NAME"));
					fk_cols.add(rs.getString("FKCOLUMN_NAME"));
					Fkey fk=new Fkey(rs.getString("PKTABLE_NAME"),rs.getString("FKTABLE_NAME"),pk_cols,fk_cols,rs.getString("DELETE_RULE"));
					fkeys.add(fk);
				
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
	
	
}