package main;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import model.DML;
import model.FKValue;

public class Combiner 
{

	public static HashMap<String,HashMap> PKValuesMap = new HashMap();
	public static HashMap<String,HashMap> FKValuesMap = new HashMap();
	
	public static void addDML(DML dml)
	{
		dml.SetPrimaryKeyValue();
    	dml.SetForeignKeyValues();
    	
		// 1.add to PKValuesMap 
    	// 1a. get table
    	HashMap<String, List<DML>> tableHashMap = PKValuesMap.get(dml.table);
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
		
		// 2. Add to FKValuesMap
		for(FKValue fkValue: dml.FKValues)
		{
			// 2a. get table
			HashMap<String, List<DML>> fkTableHashMap = FKValuesMap.get(fkValue.Referenced_Table);
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
	
	
	public static void ApplyOptimizerRules(DML dml)
	{
		HashMap<String, List<DML>> tableHashMap = PKValuesMap.get(dml.table);
		List<DML> recordDMLs = tableHashMap.get(dml.PKValue);
		
		if(OptimizerRules.checkInsertUpdateRule(dml, recordDMLs))
		{
			
		}
		else if(OptimizerRules.checkInsertDeleteRule(dml, recordDMLs))
		{
			
		}
		else if(OptimizerRules.checkUpdateDeleteRule(dml, recordDMLs))
		{
			
		}
		else if(OptimizerRules.checkUpdateUpdateRule(dml, recordDMLs))
		{
			
		}
		else if(OptimizerRules.checkDeleteInsertRule(dml, recordDMLs))
		{
			
		}
		
	}

	public static List<DML> GetRecordLevelDMLs(DML dml) {
		// TODO Auto-generated method stub
		List<DML> listOfAffectedDMls = new LinkedList<>();
		return listOfAffectedDMls;
	}

	public static void removeDML(DML affectedDML) {
		// TODO Auto-generated method stub
		
	}
}
