package model;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import main.MySqlSchemaParser;

public abstract class DML {
   public static int counter = 0;
   public static int combcounter = 0;
   public static int batchcounter = 0;

	public String DMLString;
	public String table;
	public DMLType type;
	public Map<String,String> DMLGetAttributeValues = new HashMap<String,String>(); 
	public Map<String,String> DMLSetAttributeValues = new HashMap<String,String>(); 
	public String PKValue;
	public Boolean IsRecordLevelFence = false;
	public Boolean IsTableLevelFence = false;
	public DML NextNode = null;
	public DML PrevNode = null;
	
	public DML() {
		counter++;
		combcounter++;
	}
	
	public void SetPrimaryKeyValue()
	{
		
	}
	
	public Boolean isTableLevelFence()
	{
		if (IsTableLevelFence)
			return true;
		//IF THE DML DIDN'T SPECIFY A RECORD BY PRIMARY KEY
		List<String> PKAttributes = MySqlSchemaParser.TablePkeys.get(table);
		for(String attribute: PKAttributes)
		{
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
	 
	
	public void changeValues(Map<String, String> dMLSetAttributeValues2,  DMLType newType)
	{
		type = newType;
		for (Map.Entry<String, String> entry : dMLSetAttributeValues2.entrySet()) {
		    String key = entry.getKey();
		    String value = entry.getValue().toString();
		    DMLSetAttributeValues.put(key, value);
		}
	}

	public String toDMLString() {
		return null;
		// TODO Auto-generated method stub
		
	}

	
}
