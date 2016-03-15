package model;
import java.lang.annotation.Retention;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import main.MySqlSchemaParser;

public abstract class DML {

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
	
	public void SetPrimaryKeyValue()
	{
		
	}
	
	public Boolean isTableLevelFence()
	{
		if (IsTableLevelFence)
			return true;
		//IF THE DML DIDN'T SPECIFY A RECORD BY PRIMARY KEY
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
	}
	
}
