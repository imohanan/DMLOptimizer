package util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import main.MySqlSchemaParser;

public class PrepStatement {
	public static Map<String, String> tableToInsertPreparedStatements=new HashMap<String, String>();
	public static Map<String, String> tableToDeletePreparedStatements=new HashMap<String, String>();
	
	public static void initPreparedStatementMap(){
		for(String table:MySqlSchemaParser.TablesInDB){
			tableToInsertPreparedStatements.put(table, insertPreparedStatements(table));
			tableToDeletePreparedStatements.put(table, deletePreparaedStatements(table));
		}
	}
	public static String insertPreparedStatements(String table){
		String statement=null;
		List<String> attrs=new LinkedList<String>();
		attrs=MySqlSchemaParser.getAttributes(table);
		statement="INSERT INTO "+table+" (";
		for(String attr:attrs){
			statement=statement+attr+",";
		}
		statement=statement.replaceAll(",$", "");
		statement=statement+") VALUES (";
		for (int i=0;i<attrs.size();i++){
			statement=statement+"?,";
		}
		statement=statement.replaceAll(",$", "");
		statement=statement+")";
		return statement;
		
	}
	public static String deletePreparaedStatements(String table){
		String statement=null;
		List<String> pkeys=new LinkedList<String>();
		pkeys=MySqlSchemaParser.TablePkeys.get(table);
		if (pkeys.isEmpty())
			return null;
		statement="DELETE FROM "+table+" WHERE ";
		for (String pk:MySqlSchemaParser.getAttributes(table)){
			if(pkeys.contains(pk))
				statement=statement+pk+"=? and ";
		}
		statement=statement.substring(0, statement.length()-4);
		return statement;
	}
}
