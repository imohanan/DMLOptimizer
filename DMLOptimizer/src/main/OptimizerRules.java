package main;

import java.util.List;

import model.DML;
import model.DMLQueue;
import model.DMLType;

public class OptimizerRules {

	public static boolean checkInsertUpdateRule(DML dml, List<DML> recordDMLs) {
		if (dml.type != DMLType.UPDATE)
			return false;
		
		for(DML recordDML: recordDMLs)
		{
			if (recordDML == dml)
				break;
			if (recordDML.type == DMLType.INSERT)
				return true;
		}
		return false;
	}

	
	public static boolean checkInsertDeleteRule(DML dml, List<DML> recordDMLs) {
		if (dml.type != DMLType.DELETE)
			return false;
		
		for(DML recordDML: recordDMLs)
		{
			if (recordDML == dml)
				break;
			if (recordDML.type == DMLType.INSERT)
				return true;
		}
		return false;
	}
	

	public static boolean checkUpdateDeleteRule(DML dml, List<DML> recordDMLs) {
		if (dml.type != DMLType.DELETE)
			return false;
		
		for(DML recordDML: recordDMLs)
		{
			if (recordDML == dml)
				break;
			if (recordDML.type == DMLType.UPDATE)
				return true;
		}
		return false;
	}

	
	public static boolean checkUpdateUpdateRule(DML dml, List<DML> recordDMLs) {
		if (dml.type != DMLType.UPDATE)
			return false;
		
		for(DML recordDML: recordDMLs)
		{
			if (recordDML == dml)
				break;
			if (recordDML.type == DMLType.UPDATE)
				return true;
		}
		return false;
	}

	
	public static boolean checkDeleteInsertRule(DML dml, List<DML> recordDMLs) {
		if (dml.type != DMLType.INSERT)
			return false;
		
		for(DML recordDML: recordDMLs)
		{
			if (recordDML == dml)
				break;
			if (recordDML.type == DMLType.DELETE)
				return true;
		}
		return false;
	}


	public static void applyInsertUpdateRule(DML dml, List<DML> recordDMLs) {
		for(DML recordDML: recordDMLs)
		{
			if (recordDML == dml)
				break;
			if (recordDML.type == DMLType.INSERT)
			{
				// 1. merge update values with insert
				recordDML.changeValues(dml.DMLSetAttributeValues, DMLType.INSERT);
				// 2. remove update obj from combiner and DMLQueue
				Combiner.removeDML(dml);
				DMLQueue.RemoveDML(dml);			
			}
		}
		
	}


	public static void applyInsertDeleteRule(DML dml, List<DML> recordDMLs) {
		// TODO Auto-generated method stub
		System.out.println("applyInsertDeleteRule");
		
	}


	public static void applyUpdateDeleteRule(DML dml, List<DML> recordDMLs) {
		// TODO Auto-generated method stub
		// record level DMLs removal
		// for each dML removed = remove from Queue
		// reinsert the delete dml
		System.out.println("applyUpdateDeleteRule");
	}


	public static void applyUpdateUpdateRule(DML dml, List<DML> recordDMLs) {
		// TODO Auto-generated method stub
		System.out.println("applyUpdateUpdateRule");
	}


	public static void applyDeleteInsertRule(DML dml, List<DML> recordDMLs) {
		// TODO Auto-generated method stub
		System.out.println("applyDeleteInsertRule");
	}

}
