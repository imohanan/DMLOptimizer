package test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.ResultSetMetaData;
import com.mysql.jdbc.Statement;
import com.sun.management.OperatingSystemMXBean;



import main.Main;
import main.MySqlSchemaParser;

public class OriginialRun {

	public static boolean orig=false;

	public static final long MEGABYTE = 1024L * 1024L;

	public static long bytesToMegabytes(long bytes) {
		return bytes / MEGABYTE;
	}
	

	private static Connection db_conn = null;
	public static void main(String[] args) throws IOException, SQLException{

		orig=true;
		PrintWriter fw;
		File f=new File("stats_orig.txt");
		if (!f.exists()){
			f.createNewFile();
		}
		else{
			f.delete();
		}
		try {
			fw = new PrintWriter(f);
			util.Utilization.OSStatThread osThread = new util.Utilization.OSStatThread(fw);

			System.out.println("Starting listener");
			osThread.start();
			
			System.out.println("Computing");
		OperatingSystemMXBean operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
	    RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
	    int availableProcessors = operatingSystemMXBean.getAvailableProcessors();
	    long prevUpTime = runtimeMXBean.getUptime();
	    long prevProcessCpuTime = operatingSystemMXBean.getProcessCpuTime();

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
			    try
			    {
			    	while ((line = reader.readLine()) != null) {
			    	Statement statement=(Statement) db_conn.createStatement();
			    	statement.execute(line);
			    	count++;
			    	statement.close();
			    	}
			    System.out.println("Number of executed dmls in Original way: "+ count);
			    long stopTime = System.currentTimeMillis();
			    double elapsedTime = (((stopTime - startTime)*1.67)/100000);
			    System.out.println("Time taken for original run: " + elapsedTime +" minutes");
			    }
			    catch(Exception x)
				{
			    	System.out.println(line);
				    System.err.format("Exception: %s%n", x);
				    System.out.println("Exeception");
				}
		}
		osThread.setEnd();
		runUtilization();
		 db_conn.close();

		Runtime runtime = Runtime.getRuntime();
		runtime.gc();
	    // Calculate the used memory
	    long memory = runtime.totalMemory() - runtime.freeMemory();
	    System.out.println("Used memory is bytes: " + memory);
	    double cpuUsage;
	    operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
	    long upTime = runtimeMXBean.getUptime();
	    long processCpuTime = operatingSystemMXBean.getProcessCpuTime();
	    long elapsedCpu = processCpuTime - prevProcessCpuTime;
	    long elapsedTime = upTime - prevUpTime;

	    cpuUsage = Math.min(99F, elapsedCpu / (elapsedTime * 10000F * availableProcessors));
	    System.out.println("Java CPU: " + cpuUsage);
	    System.out.println(operatingSystemMXBean.getSystemCpuLoad());
	    
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
public static void runUtilization() throws IOException, SQLException{
	File file=new File("Original_accuracy_"+Main.db+".txt");
	if (!file.exists()) {
		file.createNewFile();
	}
	else{
		file.delete();
	}
	FileWriter fw = new FileWriter(file.getAbsoluteFile());
	BufferedWriter bw = new BufferedWriter(fw);
	String fileName = "queries.txt";
	String line = null;
	 Statement st=(Statement) db_conn.createStatement();
		ResultSet rs=null;
	FileReader fileReader = 
            new FileReader(fileName);
	BufferedReader bufferedReader = 
            new BufferedReader(fileReader);
	 while((line = bufferedReader.readLine()) != null) {
			 rs=null;
			 rs=(ResultSet) st.executeQuery(line.trim());
			 ResultSetMetaData rsmd = (ResultSetMetaData) rs.getMetaData();
			 int numberOfColumns = rsmd.getColumnCount();
			 bw.append(line.trim()+"\n");
			 while(rs.next()){
				 for (int i = 1; i <= numberOfColumns; i++) {
			          if (i > 1) bw.append(",  ");
			          String columnValue = rs.getString(i);
			          bw.append(columnValue);
			        }
			 }
			 bw.append("\n");
			 
     }   

     // Always close files.
     bufferedReader.close(); 
     bw.close();
 }

}



