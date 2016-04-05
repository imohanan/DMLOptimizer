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
				return;
			}
		}
		
	}


	public static void applyInsertDeleteRule(DML dml, List<DML> recordDMLs) {
		for(DML recordDML: recordDMLs)
		{
			if (recordDML == dml) return; // TODO: use FKeys to remove FKey DMLs instead of this line
			Combiner.removeDML(recordDML);
			DMLQueue.RemoveDML(recordDML);
		}
	}


	public static void applyUpdateDeleteRule(DML dml, List<DML> recordDMLs) {
		for(DML recordDML: recordDMLs)
		{
			if (recordDML == dml)
				return;
			Combiner.removeDML(recordDML);
			DMLQueue.RemoveDML(recordDML);
		}
	}


	public static void applyUpdateUpdateRule(DML dml, List<DML> recordDMLs) {
		for(DML recordDML: recordDMLs)
		{
			if (recordDML == dml)
				break;
			if (recordDML.type == DMLType.UPDATE)
			{
				// 1. merge update values with old update values
				recordDML.changeValues(dml.DMLSetAttributeValues, DMLType.UPDATE);
				// 2. remove update obj from combiner and DMLQueue
				Combiner.removeDML(dml);
				DMLQueue.RemoveDML(dml);
				return;
			}
		}
	}


}
