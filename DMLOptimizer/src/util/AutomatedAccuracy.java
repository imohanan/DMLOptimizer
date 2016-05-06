package util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import com.mysql.jdbc.ResultSet;
import com.mysql.jdbc.ResultSetMetaData;
import com.mysql.jdbc.Statement;

import main.Main;
import main.MySqlSchemaParser;
import test.OriginialRun;

public class AutomatedAccuracy {

	public static void countStarAllTables() throws SQLException, IOException{
		String fileName=null;
		if(Main.prepared)
			fileName="prepared_accuracy_"+Main.db+".txt";
		else if(OriginialRun.orig)
			fileName="Original_accuracy_"+Main.db+".txt";
		else if(Main.manual)
			fileName="manual_accuracy_"+Main.db+".txt";
		File file = new File(fileName);
		File queries=new File("queries.txt");
		if (!file.exists()) {
			file.createNewFile();
		}
		else{
			file.delete();
		}
		if(!queries.exists()){
			queries.createNewFile();
		}
		else{
			queries.delete();
		}
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		FileWriter fw_q = new FileWriter(queries.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		BufferedWriter bw_q = new BufferedWriter(fw_q);
		Statement st=(Statement) MySqlSchemaParser.db_conn.createStatement();
		ResultSet rs=null;
		for (String t:MySqlSchemaParser.TablesInDB){
			String q="select count(*) from "+t+";";
			 rs=(ResultSet) st.executeQuery(q);
			 ResultSetMetaData rsmd = (ResultSetMetaData) rs.getMetaData();
			 int numberOfColumns = rsmd.getColumnCount();
			 bw.append(q);
			 bw.newLine();
			 bw_q.append(q);
			 bw_q.newLine();
			 
			 while(rs.next()){
				 for (int i = 1; i <= numberOfColumns; i++) {
			          if (i > 1) bw.append(",  ");
			          String columnValue = rs.getString(i);
			          bw.append(columnValue);
			        }
			 }
			 bw.newLine();
			Map<String,String> attrs=MySqlSchemaParser.AttrTypes.get(t);
			rs=null;
			for (String attr:attrs.keySet()){
				
				if (MySqlSchemaParser.numericTypes.contains(attrs.get(attr))&&!MySqlSchemaParser.TablePkeys.get(t).contains(attrs.get(attr))){
					String q2= "select count(*), "+attr+" from "+t+" group by "+attr+";";
					rs=(ResultSet) st.executeQuery(q2);
					 ResultSetMetaData rsmd1 = (ResultSetMetaData) rs.getMetaData();
					 int numberOfColumns1 = rsmd1.getColumnCount();
					 bw.append(q2);
					 bw.newLine();
					 bw_q.append(q2);
					 bw_q.newLine();
					 while(rs.next()){
						 for (int i = 1; i <= numberOfColumns1; i++) {
					          if (i > 1) bw.append(",  ");
					          String columnValue = rs.getString(i);
					          bw.append(columnValue);
					        }
					 }
					 bw.newLine();
				}
			}
			rs=null;
			String q3="select * from "+t+" limit 5;";
			 rs=(ResultSet) st.executeQuery(q3);
			 ResultSetMetaData rsmd2 = (ResultSetMetaData) rs.getMetaData();
			 int numberOfColumns2 = rsmd2.getColumnCount();
			 bw.append(q3);
			 bw.newLine();
			 bw_q.append(q3);
			 bw_q.newLine();
			 while(rs.next()){
				 for (int i = 1; i <= numberOfColumns2; i++) {
			          if (i > 1) bw.append(",  ");
			          String columnValue = rs.getString(i);
			          bw.append(columnValue);
			        }
			 }
			 bw.newLine();
		}
		bw.flush();
		bw.close();
		bw_q.flush();
		bw_q.close();
		fw.close();
		fw_q.close();
		
	}

}
