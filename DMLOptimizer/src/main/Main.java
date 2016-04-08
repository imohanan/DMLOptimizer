package main;
import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import model.DML;
import model.DMLQueue;
import model.DeleteDML;
import model.InsertDML;
import model.UpdateDML;
import util.Stats;
import util.Util;

public class Main {
	public static boolean blind=false;
	public static void main(String[] args) throws SQLException {

		// 1. Init
		MySqlSchemaParser.init_Schema(args[0],args[1],args[2]);
		Combiner combiner = new Combiner();
		Stats stats = new Stats();
		Stats.startTime = System.currentTimeMillis();
		// 2. For each log line
		
		Path filePath = Paths.get(args[3]);
		Charset charset = Charset.forName("US-ASCII");
		try (BufferedReader reader = Files.newBufferedReader(filePath, charset)) {
		    String line = null;
		    while ((line = reader.readLine()) != null) {		    	
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
		    		dml.SetForeignKeyValues();
		    		DMLQueue.AddDML(dml);
		    		Combiner.addDML(dml);
		    		
			    	if (dml.isTableLevelFence())
			        {
			    		Stats.tableFenceCount++;
			    		if(!blind)
			        	Util.BatchAndPush(); // TODO: FUTURE - Push only the impacted tables DMLs
			        	if (blind)
			        		Util.blindBatch();
			        }
			        else if (dml.isRecordLevelFence())
			        {
			        	Stats.recordFenceCount++;
			        	List<DML> listOfAffectedDMLs = Combiner.removeRecordDMLs(dml);
			        	Util.BatchAndPush(listOfAffectedDMLs);
			        	//if(!blind)
			    		//Util.BatchAndPush();
			    		//if(blind)
			    		//Util.blindBatch();
			        }
			        else
			        {
			        	Combiner.applyOptimizerRules(dml);
			        }
		    	}	        
		    }
		    if(!blind)
		    	Util.BatchAndPush();
		    if (blind)
		    	Util.blindBatch();
		} 
		catch(Exception x)
		{
		    System.err.format("Exception: %s%n", x);
		    System.out.println("Exeception");
		}
		finally
		{
			Stats.stopTime = System.currentTimeMillis();
			Stats.printStats();			
		} 
		
	}
}
