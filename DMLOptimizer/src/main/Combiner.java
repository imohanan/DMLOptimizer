package main;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import model.DML;
import model.DMLComparator;
import model.DMLQueue;
import model.FKValue;

public class Combiner 
{
	public static Map<String,Map<String, List<DML>>> PKValuesMap = new HashMap<String,Map<String, List<DML>>> ();
	public static Map<String,Map<String, List<DML>>> FKValuesMap = new HashMap<String,Map<String, List<DML>>> ();
	
	public static void addDML(DML dml)
	{
		Main.batcher.DMLAfterCombining++;
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
			Main.batcher.insertUpdateCount++;
			OptimizerRules.applyInsertUpdateRule(dml, recordDMLs);
		}
		else if(OptimizerRules.checkInsertDeleteRule(dml, recordDMLs))
		{
			Main.batcher.insertDeleteCount++;
			OptimizerRules.applyInsertDeleteRule(dml, recordDMLs, fkDMLs); 
		}
		else if(OptimizerRules.checkUpdateDeleteRule(dml, recordDMLs))
		{
			Main.batcher.updateDeleteCount++;
			OptimizerRules.applyUpdateDeleteRule(dml, recordDMLs, fkDMLs); 
		}
		else if(OptimizerRules.checkUpdateUpdateRule(dml, recordDMLs))
		{
			Main.batcher.updateUpdateCount++;
			OptimizerRules.applyUpdateUpdateRule(dml, recordDMLs); 
		}
		
	}

	
	public static void removeDML(DML dml) {
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


	public static PriorityQueue<DML> removeRecordDMLs(DML dml) {
		
		Map<String, List<DML>> tableMap = PKValuesMap.get(dml.table);
		List<DML> recordDMLs = tableMap.get(dml.PKValue);

		List<DML> FKrecordDMLs = Collections.emptyList();
		if (FKValuesMap.containsKey(dml.table))
		{
			Map<String, List<DML>> FKtableMap = FKValuesMap.get(dml.table);
			if (FKtableMap.containsKey(dml.PKValue))
				FKrecordDMLs = FKtableMap.get(dml.PKValue);
		}
		int size = recordDMLs.size() + FKrecordDMLs.size();
		PriorityQueue<DML> finalDMLMinHeap = new PriorityQueue<DML>(size, new DMLComparator());
		
		while(recordDMLs.size() != 0)
		{
			DML deleteElement = recordDMLs.get(0);
			finalDMLMinHeap.add(deleteElement);
			removeDML(deleteElement);
			DMLQueue.RemoveDML(deleteElement);
		}
		while(FKrecordDMLs.size() != 0)
		{
			DML deleteElement = FKrecordDMLs.get(0);
			finalDMLMinHeap.add(deleteElement);
			removeDML(deleteElement);
			DMLQueue.RemoveDML(deleteElement);
		}
		
		return finalDMLMinHeap;
	}
	
}
