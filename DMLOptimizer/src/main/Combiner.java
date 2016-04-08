package main;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import model.DML;
import model.FKValue;
import util.Stats;

public class Combiner 
{
	public static Map<String,Map<String, List<DML>>> PKValuesMap = new HashMap<String,Map<String, List<DML>>> ();
	public static Map<String,Map<String, List<DML>>> FKValuesMap = new HashMap<String,Map<String, List<DML>>> ();
	
	public static void addDML(DML dml)
	{
		Stats.DMLAfterCombining++;
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
		
		// 2. Add to FK Map
		for(FKValue fkValue: dml.FKValues)
		{
			// 2a. get table
			Map<String, List<DML>> fkTableHashMap = FKValuesMap.get(fkValue.Referenced_Table);
			if (fkTableHashMap == null)
			{
				FKValuesMap.put(fkValue.Referenced_Table, new HashMap<String, List<DML>>());
				fkTableHashMap = FKValuesMap.get(fkValue.Referenced_Table);
			}
						
			//2. add DML to FK
			List<DML> fkRecordDMLs = fkTableHashMap.get(fkValue.FKValueString);
			if (fkRecordDMLs == null)
			{
				fkRecordDMLs = new LinkedList<DML>();
			}
			fkRecordDMLs.add(dml);
			fkTableHashMap.put(fkValue.FKValueString, fkRecordDMLs);
		}	
	}
	
	
	public static void applyOptimizerRules(DML dml)
	{
		Map<String, List<DML>> tableHashMap = PKValuesMap.get(dml.table);
		List<DML> recordDMLs = tableHashMap.get(dml.PKValue);

		Map<String, List<DML>> fkHashMap = FKValuesMap.get(dml.table);
		List<DML> fkDMLs = fkHashMap.get(dml.PKValue);
	
		// NO DMLs to reduce against
		if(recordDMLs.size() == 1)
			return;
		
		if(OptimizerRules.checkInsertUpdateRule(dml, recordDMLs))
		{
			Stats.insertUpdateCount++;
			OptimizerRules.applyInsertUpdateRule(dml, recordDMLs);
		}
		else if(OptimizerRules.checkInsertDeleteRule(dml, recordDMLs))
		{
			Stats.insertDeleteCount++;
			OptimizerRules.applyInsertDeleteRule(dml, recordDMLs, fkDMLs); 
		}
		else if(OptimizerRules.checkUpdateDeleteRule(dml, recordDMLs))
		{
			Stats.updateDeleteCount++;
			OptimizerRules.applyUpdateDeleteRule(dml, recordDMLs, fkDMLs); 
		}
		else if(OptimizerRules.checkUpdateUpdateRule(dml, recordDMLs))
		{
			Stats.updateUpdateCount++;
			OptimizerRules.applyUpdateUpdateRule(dml, recordDMLs); 
		}
		
	}

	
	
	public static void removeDML(DML dml) {
		Stats.DMLAfterCombining--;
		Map<String, List<DML>> tableMap = PKValuesMap.get(dml.table);
		List<DML> recordDMLs = tableMap.get(dml.PKValue);
		recordDMLs.remove(dml); //TEST: test if this is reflected in PKValuesMap
		
		//FKeys removal
	}


	/*public static List<DML> removeRecordDMLs(DML dml) {
		Map<String, List<DML>> tableMap = PKValuesMap.get(dml.table);
		List<DML> recordDMLs = tableMap.get(dml.PKValue);
		
		for(DML deleteDML: recordDMLs)
			removeDML(deleteDML);
		
		return recordDMLs;
	}*/
	
}
