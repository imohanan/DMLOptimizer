package util;

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
	
	public static void printStats()
	{
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
	}
}
