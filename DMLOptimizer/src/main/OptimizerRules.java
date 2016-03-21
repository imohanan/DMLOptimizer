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
				return;
			}
		}
		
	}


	public static void applyInsertDeleteRule(DML dml, List<DML> recordDMLs, List<DML> fkDMLs) {
		for(DML fkDML: fkDMLs)
		{
			Combiner.removeDML(fkDML);
			DMLQueue.RemoveDML(fkDML);
		}
		for(DML recordDML: recordDMLs)
		{
			Combiner.removeDML(recordDML);
			DMLQueue.RemoveDML(recordDML);
		}
	}


	public static void applyUpdateDeleteRule(DML dml, List<DML> recordDMLs, List<DML> fkDMLs) {
		for(DML fkDML: fkDMLs)
		{
			Combiner.removeDML(fkDML);
			DMLQueue.RemoveDML(fkDML);
		}
		for(DML recordDML: recordDMLs)
		{
			if (recordDML == dml)
				continue;
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


	public static void applyDeleteInsertRule(DML dml, List<DML> recordDMLs, List<DML> fkDMLs) {
		// DELETE MAY RESULT IN THE REMOVAL OF OTHER fkS IN db AND SHOULD BE EXECUTED
		for(DML fkDML: fkDMLs)
		{
			Combiner.removeDML(fkDML);
			DMLQueue.RemoveDML(dml);
		}
		
		for(DML recordDML: recordDMLs)
		{
			if( recordDML.type != DMLType.DELETE && recordDML.type != DMLType.INSERT)
			{
				Combiner.removeDML(recordDML);
				DMLQueue.RemoveDML(recordDML);
			}
		}
	}

}
