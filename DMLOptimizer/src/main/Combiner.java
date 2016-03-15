package main;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import model.DML;

public class Combiner 
{

	public static HashMap<String,HashMap> PKValuesMap = new HashMap();
	public static HashMap<String,HashMap> FKValuesMap = new HashMap();
	
	public static void addDML(DML dml)
	{
		// ToDo
		// 1.add to PKValuesMap
		// 2. add to FKValuesMap
	}
	
	

	public static List<DML> GetRecordLevelDMLs(DML dml) {
		// TODO Auto-generated method stub
		List<DML> listOfAffectedDMls = new ArrayList<>();
		return listOfAffectedDMls;
	}

	public static void removeDML(DML affectedDML) {
		// TODO Auto-generated method stub
		
	}
}
