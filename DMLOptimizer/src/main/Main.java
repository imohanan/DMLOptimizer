package main;
import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import com.mysql.jdbc.PreparedStatement;

import model.DML;
import model.DMLQueue;
import model.DeleteDML;
import model.InsertDML;
import model.UpdateDML;
import util.PrepStatement;
import util.Stats;
import util.Util;

public class Main {
	public static boolean blind=false;
	public static boolean prepared=true;
	public static Batcher batcher;
	static int counter = 0;
	
	public static void main(String[] args) throws SQLException {

		// 1. Init
		if (blind)
			batcher = new BlindBatcher();
		else if (prepared)
			batcher = new PreparedBatcher();
		else
			batcher = new ManualBatcher();
		
		MySqlSchemaParser.init_Schema(args[0],args[1],args[2]);
		PrepStatement.initPreparedStatementMap();
		Combiner combiner = new Combiner();
		batcher.startTime = System.currentTimeMillis();
		// 2. For each log line
		
		Path filePath = Paths.get(args[3]);
		Charset charset = Charset.forName("US-ASCII");
		try (BufferedReader reader = Files.newBufferedReader(filePath, charset)) {
		    String line = null;
		    while ((line = reader.readLine()) != null) {
		    	counter++;
		    	System.out.println(counter);
		    	if (counter == 2)
		    		System.out.println("Debug");
		    	String[] splitDMLLines = Util.splitDMLsByOR(line);
		    	for(String dmlLine: splitDMLLines)
		    	{
		    		//dmlLine = Util.preprocessDMLString(dmlLine);
		    		DML dml;
		    		String[] words = dmlLine.split(" ");
		    		if (words[0].equalsIgnoreCase("insert"))
		    			dml = new InsertDML(dmlLine);
		    		else if (words[0].equalsIgnoreCase("delete"))
		    			dml = new DeleteDML(dmlLine);
		    		else if (words[0].equalsIgnoreCase("update"))
		    			dml = new UpdateDML(dmlLine);
		    		else
		    			continue;
		    		
		    		dml.SetPrimaryKeyValue();
		    		dml.SetForeignKeyValues();
		    		DMLQueue.AddDML(dml);
		    		Combiner.addDML(dml);
		    		
			    	if (dml.isTableLevelFence())
			        {
			    		batcher.tableFenceCount++;
			    		batcher.BatchAndPush();
			        }
			        else if (dml.isRecordLevelFence())
			        {
			        	batcher.recordFenceCount++;
			        	PriorityQueue<DML> affectedDMLs = Combiner.removeRecordDMLs(dml);
			        	batcher.BatchAndPush(affectedDMLs);
			        }
			        else
			        {
			        	Combiner.applyOptimizerRules(dml);
			        }
		    	}	        
		    }
		    batcher.BatchAndPush();
		} 
		catch(Exception x)
		{
		    System.err.format("Exception: %s%n", x);
		}
		finally
		{
			batcher.stopTime = System.currentTimeMillis();
			batcher.printStats();			
		} 
		
	}
}
