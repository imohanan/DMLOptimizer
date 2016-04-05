package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.Statement;

public class OriginialRun {
	
	private static Connection db_conn = null;
	public static void main(String[] args) throws IOException, SQLException{
		long startTime = System.currentTimeMillis();
		if (args.length<3){
			System.out.println("Wrong number of arguments.Exiting.");
			System.exit(-1);
		}
		else{
			int count=0;
			String line = null;
			setupConnection(args[0], args[1], args[2]);
			Path filePath = Paths.get(args[3]);
			Charset charset = Charset.forName("US-ASCII");
			BufferedReader reader = Files.newBufferedReader(filePath, charset);
			    while ((line = reader.readLine()) != null) {
			    	Statement statement=(Statement) db_conn.createStatement();
			    	statement.execute(line);
			    	count++;
			    	statement.close();
			    	}
			    System.out.println("Number of executed dmls in Original way: "+ count);
			    db_conn.close();
			    long stopTime = System.currentTimeMillis();
			    double elapsedTime = (((stopTime - startTime)*1.67)/100000);
			    System.out.println("Time taken for original run: " + elapsedTime +" minutes");
			    }
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
							"jdbc:mysql://localhost:3306/"+db, username, password);
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
	public static boolean doesDBExist(String db) throws SQLException{
		ResultSet rs=db_conn.getMetaData().getCatalogs();
		while (rs.next()){
			if (rs.getString(1).equalsIgnoreCase(db))
				return true;
		}
		return false;
	}


}
