package main;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

public class Combiner 
{

	public static HashMap<String,HashMap> PKValuesMap = new HashMap();
	public static HashMap<String,HashMap> FKValuesMap = new HashMap();
	
	public void addDML(DML dml)
	{
		// ToDo
		// 1.add to PKValuesMap
		// 2. add to FKValuesMap
	}
	
	public List<DML> getRLFDMLs( DML dml)
	{
		List<DML> listOfAffectedDMls = new ArrayList<>();
		return listOfAffectedDMls;
		
	}
}
