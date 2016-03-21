package model;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import main.MySqlSchemaParser;

public abstract class DML {

	public String DMLString;
	public String table;
	public DMLType type;
	public Map<String,String> DMLGetAttributeValues = new HashMap<String,String>(); 
	public Map<String,String> DMLSetAttributeValues = new HashMap<String,String>(); 
	public String PKValue;
	public List<FKValue> FKValues = new LinkedList<FKValue>();
	public Boolean IsRecordLevelFence = false;
	public Boolean IsTableLevelFence = false;
	public DML NextNode = null;
	public DML PrevNode = null;
	
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
	
	public void toDMLString()
	{
	}
	
	public void SetForeignKeyValues()
	{
		List<Fkey> FKeys= MySqlSchemaParser.TableFkeys.get(table);
		for(Fkey fk: FKeys)
		{
			String fk_table = fk.getFk_table();
			List<String> columns = fk.getFk_cols();
			String keyValue = "";
			Boolean keyFound = true;
			for(String col: columns)
			{
				String val = DMLGetAttributeValues.get(col);
				if (val == null)
				{
					keyFound = false;
					break;
				}
				keyValue = keyValue + val +";";
			}
			if (keyFound == true)
			{
				FKValue fKValue = new FKValue(keyValue,fk_table);
				FKValues.add(fKValue);
			}
		}
	}
}
