package main;
import java.util.ArrayList;
import java.util.Collections;
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

		List<DML> fkDMLs = Collections.emptyList();
		if (FKValuesMap.containsKey(dml.table))
		{
			Map<String, List<DML>> fkHashMap = FKValuesMap.get(dml.table);
			if (fkHashMap.containsKey(dml.PKValue))
				fkDMLs = fkHashMap.get(dml.PKValue);
		}
	
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
		for(FKValue fkValue: dml.FKValues)
		{
			Map<String, List<DML>> fkMap = FKValuesMap.get(fkValue.Referenced_Table);
			List<DML> fkdmls = fkMap.get(fkValue.FKValueString);
			fkdmls.remove(dml);
		}
	}


	public static List<DML> removeRecordDMLs(DML dml) {
		List<DML> finalDMLs = new LinkedList<DML>();
		
		Map<String, List<DML>> tableMap = PKValuesMap.get(dml.table);
		List<DML> recordDMLs = tableMap.get(dml.PKValue);

		List<DML> FKrecordDMLs = Collections.emptyList();
		if (FKValuesMap.containsKey(dml.table))
		{
			Map<String, List<DML>> FKtableMap = FKValuesMap.get(dml.table);
			if (FKtableMap.containsKey(dml.PKValue))
				FKrecordDMLs = FKtableMap.get(dml.PKValue);
		}
		
		for(DML deleteDML: recordDMLs)
		{
			finalDMLs.add(deleteDML);
			removeDML(deleteDML);
		}
		for(DML deleteDML: FKrecordDMLs)
		{
			finalDMLs.add(deleteDML);
			removeDML(deleteDML);
		}
		
		return finalDMLs;
	}
	
}
