package main;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import model.DML;
import model.DMLQueue;
import model.DeleteDML;
import model.InsertDML;
import model.UpdateDML;
import util.Util;

public class Main {

	
	
	public static void main(String[] args) {

		// 1. Init
		MySqlSchemaParser.init_Schema(args[0],args[1],args[2]);;
		Combiner combiner = new Combiner();
		
		// 2. For each log line
		Path filePath = Paths.get("./LogFiles/dmls_log.txt");
		Charset charset = Charset.forName("US-ASCII");
		try (BufferedReader reader = Files.newBufferedReader(filePath, charset)) {
		    String line = null;
		    while ((line = reader.readLine()) != null) {
		    	
		    	String[] splitDMLLines = Util.SplitDMLStrings(line);
		    	for(String dmlLine: splitDMLLines)
		    	{
		    		DML dml;
		    		String[] words = dmlLine.split(" ");
		    		if (words[0].equals("INSERT"))
		    			dml = new InsertDML(dmlLine);
		    		else if (words[0].equals("DELETE"))
		    			dml = new DeleteDML(dmlLine);
		    		else
		    			dml = new UpdateDML(dmlLine);
		    		
			    	if (dml.IsTableLevelFence)
			        {
			        	System.out.println("Table Level Fence found");
			        	Util.BatchAndPush(); // TODO: FUTURE - Push only the impacted tables DMLs
			        }
			        else if (dml.IsRecordLevelFence)
			        {
			        	System.out.println("Record Level Fence found");
			        	List<DML> ListOfAffectedDMLs = Combiner.GetRecordLevelDMLs(dml);
			        	for(DML affectedDML: ListOfAffectedDMLs)
			        	{
			        		Combiner.removeDML(affectedDML);
			        	}
			        	Util.BatchAndPush(ListOfAffectedDMLs);
			        }
			        else
			        {
			        	DMLQueue.AddDML(dml);
			        	Combiner.addDML(dml);
			        	dml.SetPrimaryKeyValue();
			        	dml.toDMLString();
			        }
		    	}	        
		    }
		    Util.BatchAndPush();
		} 
		catch (IOException x) {
		    System.err.format("IOException: %s%n", x);
		}
	}
}