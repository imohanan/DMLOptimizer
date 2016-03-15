package main;
import java.lang.annotation.Retention;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import model.DMLType;

public class DML {

	public String DMLString;
	public String table;
	public DMLType type;
	public HashMap<String,String> DMLGetAttributeValues = new HashMap(); 
	public HashMap<String,String> DMLSetAttributeValues = new HashMap(); 
	// public PK_Values;
	// public FK_Values;
	public String PKValue;
	public Boolean IsRecordLevelFence = false;
	public Boolean IsTableLevelFence = false;
	
	public DML(String inputString)
	{
		inputString = inputString.replace(';', ' ');
		inputString = inputString.trim();
		/*
		 * 1. set DML string
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
		
		if (type == DMLType.INSERT)
		{
			String values = words[4].replace('(', ' ');
			values = values.replace(')', ' ');
			values = values.trim();
			String[] valueList = values.split(",");
			for(int idx = 0; idx < valueList.length; idx++)
			{
				DMLSetAttributeValues.put(MySqlSchemaParser.TableAttrs.get(table).get(idx).toString(), valueList[idx]);
			}
		}
		else if (type == DMLType.UPDATE)
		{
			String[] clauses = inputString.split(" WHERE ");
			//TODO add the SET attributes SET PendCount=PendCount+1?? Should be record level fence?????
			String[] attVals = clauses[1].split(" AND | OR ");
			for(String attVal: attVals)
			{
				String[] elements = attVal.split("=");
				DMLGetAttributeValues.put(elements[0], elements[1].toString());
			}
		}
		else if (type == DMLType.DELETE)
		{
			String[] clauses = inputString.split(" WHERE ");
			String[] attVals = clauses[1].split(" AND ");
			for(String attVal: attVals)
			{
				String[] elements = attVal.split("=");
				DMLGetAttributeValues.put(elements[0].toString(), elements[1].toString());
			}
		}
	}
	
	
	public Boolean isTableLevelFence()
	{
		if (IsTableLevelFence)
			return true;
		//IF THE DML DIDN'T SPECIFY A RECORD BY PRIMARY KEY
		if(type == DMLType.INSERT)
			return false;
		Vector PKAttributes = MySqlSchemaParser.TablePkeys.get(table);
		for(int idx = 0; idx < PKAttributes.size(); idx++)
		{
			String attribute = PKAttributes.get(idx).toString();
			String mapValue = DMLGetAttributeValues.get(attribute);
			if(mapValue == null)
			{
				IsTableLevelFence = true;
				return true;
			}
		}
		return false;
	}
	
	
	public Boolean isRecordLevelFence()
	{
		if (IsRecordLevelFence)
			return true;
		// ALREADY VERFIED IF TABLE LEVEL FENCE, IE IF ALL pk ATTRS PRESENT
		// IF ANY ADDITIONAL ATTRIBUTES ARE PRESENT THEN TABLE LEVEL FENCE
		if(type == DMLType.INSERT)
			return false;
		if (DMLGetAttributeValues.size() >  MySqlSchemaParser.TablePkeys.get(table).size())
		{
			IsRecordLevelFence = true;
			return true;
		}
		return false;
	}
	
	
	public void changeValues(HashMap<String, String> newAttributes, DMLType newType)
	{
		type = newType;
		for (Map.Entry<String, String> entry : newAttributes.entrySet()) {
		    String key = entry.getKey();
		    String value = entry.getValue().toString();
		    DMLSetAttributeValues.put(key, value);
		}
	}
	
	public void toDMLString()
	{
		if(type == DMLType.INSERT)
			//TODO
			System.out.println(type);
		else if(type == DMLType.DELETE)
			//TODO
			System.out.println(type);
		else if(type == DMLType.UPDATE)
			//TODO
			System.out.println(type);
	}
	
}

