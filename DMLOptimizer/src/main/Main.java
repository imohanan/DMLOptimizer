package main;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

import com.sun.management.OperatingSystemMXBean;

import model.DML;
import model.DMLQueue;
import model.DeleteDML;
import model.InsertDML;
import model.UpdateDML;
import test.OriginialRun;
import util.PrepStatement;
import util.SystemStats;
import util.Util;

public class Main {

	public static boolean prepared=false;
	public static boolean manual=false;
	public static Batcher batcher;
	public static String db=null;

	public static void main(String[] args) throws SQLException, IOException {
		OriginialRun.orig=false;
		
		// 1. Init		
		Combiner combiner = new Combiner();
		MySqlSchemaParser.init_Schema(args[0],args[1],args[2]);
		SystemStats systemStats = new SystemStats();
		prepared = Boolean.parseBoolean(args[4]);
		manual = !prepared;
		db=args[2];
		if (prepared)
			batcher = new PreparedBatcher();
		else if(manual)
			batcher = new ManualBatcher();

		// 2. For each log line
		try (BufferedReader reader = Files.newBufferedReader(Paths.get(args[3]), Charset.forName("US-ASCII"))) {
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
			batcher.stopTime = System.currentTimeMillis();
		    util.AutomatedAccuracy.countStarAllTables();
		} 
		catch(Exception x)
		{
		    System.err.format("Exception: %s%n", x);
		}
		finally
		{
			systemStats.stop();
			batcher.printStats();			
		} 
		
	}
}

