package main;
import java.util.HashMap;

import model.DMLType;

public class DML {

	public String DMLString;
	public String table;
	public DMLType type;
	public HashMap<String,String> attributeValues;
	// public PK_Values;
	// public FK_Values;
	public String PKValue;
	
	public DML(String inputString)
	{
		/*
		 * 1. set DMl string
		 * 2. Set type
		 * 3. set table
		 * 4. set attributes Values
		 * 5. use Schema to set PrimaryKey 
		 */
		DMLString = inputString;
		
		String[] words = inputString.split(" ");
		if (words[0].equals("INSERT"))
			type = DMLType.INSERT;
		else if(words[0].equals("DELETE"))
			type = DMLType.DELETE;
		else if(words[0].equals("UPDATE"))
			type = DMLType.UPDATE;
		
		if (type == DMLType.INSERT)
			table = words[2];
		else if (type == DMLType.UPDATE)
			table = words[1];
		else if (type == DMLType.DELETE)
			table = words[2];
		
		
		
		
		
		
	}
	
	public Boolean isRecordLevelFence()
	{
		// ToDo
		return false;
	}
	
	public Boolean isTableLevelFence()
	{
		//ToDo
		return false;
	}
	
	public void changeValues(HashMap<String, String> newAttributes, DMLType type)
	{
		// ToDo
	}
	
	public void toDMLString()
	{
		//Update DMlString
	}
	
}

