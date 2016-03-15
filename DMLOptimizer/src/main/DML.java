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
		/*
		 * 1. set DML string
		 * 2. Set type
		 * 3. set table
		 * 4. set attributes Values 
		 */
		// 1. set DML string
		DMLString = inputString;
		inputString = inputString.replace(';', ' ');
		inputString = inputString.trim();
		
		// 2. Set type
		String[] words = inputString.split(" ");
		if (words[0].equals("INSERT"))
			type = DMLType.INSERT;
		else if(words[0].equals("DELETE"))
			type = DMLType.DELETE;
		else if(words[0].equals("UPDATE"))
			type = DMLType.UPDATE;
		
		// 3. set table
		if (type == DMLType.INSERT)
			table = words[2];
		else if (type == DMLType.UPDATE)
			table = words[1];
		else if (type == DMLType.DELETE)
			table = words[2];
		
		// 4. set attributes Values
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
			
			String[] setClauses = clauses[0].split(" SET ");
			String[] setAttValues = setClauses[1].split(",");
			for(String setAttVal: setAttValues)
			{
				String[] elements = setAttVal.trim().split("=");
				DMLSetAttributeValues.put(elements[0], elements[1].toString());
			}
			
			String[] attVals = clauses[1].split(" AND ");
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
	
	
	public void SetPrimaryKeyValue()
	{
		PKValue = "";
		Vector<String> PKAttributes = MySqlSchemaParser.TablePkeys.get(table);
		if (type == DMLType.DELETE || type == DMLType.UPDATE)
		{
			for(int idx=0; idx < PKAttributes.size(); idx++)
			{
				PKValue += DMLGetAttributeValues.get(PKAttributes.get(idx)) + ";";
			}
		}
		else if (type == DMLType.INSERT)
		{
			for(int idx=0; idx < PKAttributes.size(); idx++)
			{
				PKValue += DMLSetAttributeValues.get(PKAttributes.get(idx)) + ";";
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
		Vector<String> PKAttributes = MySqlSchemaParser.TablePkeys.get(table);
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
		{
			String attributes = "(";
			String values = "(";
			for(Map.Entry<String, String> entry :DMLSetAttributeValues.entrySet())
			{
				attributes = attributes + entry.getKey() + ",";
				values = values + entry.getValue() + ",";
			}
			attributes = attributes.substring(0, attributes.length() - 1) + ")";
			values = values.substring(0, values.length() - 1) + ")";
			DMLString = "INSERT INTO " + table + attributes + " VALUES " + values + ";";
		}
		else if(type == DMLType.DELETE)
		{
			String whereClause = "";
			for(Map.Entry<String, String> entry: DMLGetAttributeValues.entrySet() )
			{
				whereClause = whereClause + entry.getKey() + "=" + entry.getValue() + " AND ";
			}
			whereClause = whereClause.substring(0, whereClause.length() - 5);
			DMLString = "DELETE FROM " + table + " WHERE " + whereClause + ";";
		}
		else if(type == DMLType.UPDATE)
		{
			String setClause = "";
			String whereClause = "";
			for(Map.Entry<String, String> entry :DMLSetAttributeValues.entrySet())
			{
				setClause = setClause + entry.getKey() +"=" + entry.getValue()+ ",";
			}
			setClause = setClause.substring(0, setClause.length() - 1);
			for(Map.Entry<String, String> entry: DMLGetAttributeValues.entrySet() )
			{
				whereClause = whereClause + entry.getKey() + "=" + entry.getValue() + " AND ";
			}
			whereClause = whereClause.substring(0, whereClause.length() - 5);
			DMLString = "UPDATE " + table + " SET " + setClause + " WHERE " + whereClause + ";";
		}
	}
	
}
