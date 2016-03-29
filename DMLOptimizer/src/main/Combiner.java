package main;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import model.DML;

public class Combiner 
{

	public static Map<String,Map<String, List<DML>>> PKValuesMap = new HashMap<String,Map<String, List<DML>>> ();
	
	public static void addDML(DML dml)
	{
    	
		// 1.add to PKValuesMap 
    	// 1a. get table
    	Map<String, List<DML>> tableHashMap = PKValuesMap.get(dml.table);
    	if (tableHashMap == null)
    	{
    		PKValuesMap.put(dml.table, new HashMap<String, List<DML>>() );
    		tableHashMap = PKValuesMap.get(dml.table);
    	}
    	
    	// 1b.add DML to PK 
    	List<DML> recordDMLs = tableHashMap.get(dml.PKValue);
    	if (recordDMLs == null)
    	{
    		recordDMLs = new LinkedList<DML>();
    	}
    	recordDMLs.add(dml);
		tableHashMap.put(dml.PKValue, recordDMLs);
		
	}
	
	
	public static void applyOptimizerRules(DML dml)
	{
		Map<String, List<DML>> tableHashMap = PKValuesMap.get(dml.table);
		List<DML> recordDMLs = tableHashMap.get(dml.PKValue);
		
		// NO DMLs to reduce against
		if(recordDMLs.size() == 1)
			return;
		
		if(OptimizerRules.checkInsertUpdateRule(dml, recordDMLs))
		{
			OptimizerRules.applyInsertUpdateRule(dml, recordDMLs);
		}
		else if(OptimizerRules.checkInsertDeleteRule(dml, recordDMLs))
		{
			OptimizerRules.applyInsertDeleteRule(dml, recordDMLs);
		}
		else if(OptimizerRules.checkUpdateDeleteRule(dml, recordDMLs))
		{
			OptimizerRules.applyUpdateDeleteRule(dml, recordDMLs);
		}
		else if(OptimizerRules.checkUpdateUpdateRule(dml, recordDMLs))
		{
			OptimizerRules.applyUpdateUpdateRule(dml, recordDMLs); 
		}
		
	}

	
	
	public static void removeDML(DML dml) {
		Map<String, List<DML>> tableMap = PKValuesMap.get(dml.table);
		List<DML> recordDMLs = tableMap.get(dml.PKValue);
		recordDMLs.remove(dml); //TEST: test if this is reflected in PKValuesMap
	}


	public static List<DML> removeRecordDMLs(DML dml) {
		Map<String, List<DML>> tableMap = PKValuesMap.get(dml.table);
		List<DML> recordDMLs = tableMap.get(dml.PKValue);
		
		for(DML deleteDML: recordDMLs)
			removeDML(deleteDML);
		
		return recordDMLs;
	}
	
}
