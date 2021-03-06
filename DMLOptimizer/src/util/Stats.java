package util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import main.Main;

public class Stats {
	
	public static long startTime = 0;
	public static long stopTime = 0;
	public static double TotalTime = 0;
	
	public static int DMLTotal = 0;
	public static int DMLAfterCombining = 0;
	
	public static int insertUpdateCount = 0;
	public static int insertDeleteCount = 0;
	public static int updateDeleteCount = 0;
	public static int updateUpdateCount = 0;
	public static int pendcountDML = 0 ;
	
	public static int recordFenceCount = 0;
	public static int tableFenceCount = 0;
	public static int totalBatched=1;
	
	public static int maxCombinerToBatchSize=Integer.MIN_VALUE;
	public static int minCombinerToBatchSize=Integer.MAX_VALUE;
	public static int batchCalls=0;

	public static int minBatched= Integer.MAX_VALUE;//Min batched from any size of group coming from combiner
	public static int maxBatched=1;
    
	public static int dbmsAccess=0;
	public static int countManualBatcher=0; 
	public static boolean issueToDBMS=true;
	public static Map<Integer, Integer> NoDMLsPassedToBatcher=new HashMap<Integer,Integer>();//<size of group,occurance>
	public static Map<Integer, Integer> BatchSizeToDBMSAccess=new HashMap<Integer,Integer>();//<size of group,#dbms access>
	
	public static void printStats()
	{		
		float avgbatch ;
		double elapsedTime = (((stopTime - startTime)*1.67)/100000);
	    System.out.println("Time taken in Optimized algorithm: " + elapsedTime +" minutes");
	    
		System.out.println("Total Number of DMLs: " + Stats.DMLTotal);
		System.out.println("Number of DMLs after combining: " + Stats.DMLAfterCombining);
		
		System.out.println("Table Level Fence count is: " + Stats.tableFenceCount);
    	System.out.println("Record Level Fence count is: " + Stats.recordFenceCount );
    	
    	System.out.println("Rule InsertUpdate count is : " + Stats.insertUpdateCount);
    	System.out.println("Rule InsertDelete count is : " + Stats.insertDeleteCount);
    	System.out.println("Rule UpdateDelete count is : " + Stats.updateDeleteCount);
    	System.out.println("Rule UpdateUpdate count is : " + Stats.updateUpdateCount);
    	System.out.println("Number of  PendCount type DMLs observed and combined is : " + Stats.pendcountDML);
    	
    	System.out.println("Minimum number of DMLs passed from Combiner -> Batcher in one call: "+Integer.toString(minCombinerToBatchSize));
    	System.out.println("maximum number of DMLs passed from Combiner -> Batcher in one call: "+Integer.toString(maxCombinerToBatchSize));
    	System.out.println("Average number of DMLs passed from Combiner -> Batcher per call: "+Float.toString(DMLAfterCombining/batchCalls));
//    	System.out.println("Total number of DMLs after batching: "+ totalBatched);
//		System.out.println("Total number of access to dbms: "+dbmsAccess);
		System.out.println("Minimum number of DMLS in a batch: "+Integer.toString(minBatched));
		System.out.println("Maximum number of DMLS in a batch: "+Integer.toString(maxBatched));
		if (Main.prepared == true)
		{
			avgbatch = (float)DMLAfterCombining/(float)dbmsAccess;
		}
		else
		{
			avgbatch = (float)DMLAfterCombining/(float)countManualBatcher;
		}
			
		System.out.println("Average number of DMLs per batch:" + Float.toString(avgbatch));
//		System.out.println("Average number of DMLS in each batch: "+Float.toString(DMLAfterCombining/totalBatched));
	
	}
}
