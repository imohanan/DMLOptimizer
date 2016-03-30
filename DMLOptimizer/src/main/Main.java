package main;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;

import model.DML;
import model.DMLQueue;
import model.DeleteDML;
import model.InsertDML;
import model.UpdateDML;
import util.Util;

public class Main {

	
	
	public static void main(String[] args) throws SQLException {

		// 1. Init
		MySqlSchemaParser.init_Schema(args[0],args[1],args[2]);
		Combiner combiner = new Combiner();
		
		// 2. For each log line
		Path filePath = Paths.get(args[3]);
		Charset charset = Charset.forName("US-ASCII");
		try (BufferedReader reader = Files.newBufferedReader(filePath, charset)) {
		    String line = null;
		    while ((line = reader.readLine()) != null) {
		    	
		    	line = line.toLowerCase();
		    	String[] splitDMLLines = Util.splitDMLsByOR(line);
		    	for(String dmlLine: splitDMLLines)
		    	{
		    		DML dml;
		    		String[] words = dmlLine.split(" ");
		    		if (words[0].equalsIgnoreCase("insert"))
		    			dml = new InsertDML(dmlLine);
		    		else if (words[0].equalsIgnoreCase("delete"))
		    			dml = new DeleteDML(dmlLine);
		    		else
		    			dml = new UpdateDML(dmlLine);
		    		
		    		dml.SetPrimaryKeyValue();
		    		DMLQueue.AddDML(dml);
		    		
			    	if (dml.isTableLevelFence())
			        {
			        	System.out.println("Table Level Fence found");
			        	Util.BatchAndPush(); // TODO: FUTURE - Push only the impacted tables DMLs
			        }
			        else if (dml.isRecordLevelFence())
			        {
			        	System.out.println("Record Level Fence found");
			    		Combiner.addDML(dml);
//			        	List<DML> ListOfAffectedDMLs = Combiner.removeRecordDMLs(dml);
//			        	Util.BatchAndPush(ListOfAffectedDMLs);
			    		Util.BatchAndPush();
			        }
			        else
			        {

			    		Combiner.addDML(dml);
			        	Combiner.applyOptimizerRules(dml);
			        }
		    	}	        
		    }
		    Util.BatchAndPush();
		} 
		catch (IOException x) {
		    System.err.format("IOException: %s%n", x);
		}
		catch(Exception x)
		{
		    System.err.format("Exception: %s%n", x);
		}
	}
}