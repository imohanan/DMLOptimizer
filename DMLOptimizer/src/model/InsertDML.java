package model;

import java.util.List;
import java.util.Map;

import main.MySqlSchemaParser;

public class InsertDML extends DML{

	public InsertDML(String inputString) {
		//1. set String
		DMLString = inputString;
		// 2. Set Type
		type = DMLType.INSERT;
		
		inputString = inputString.replace(';', ' ');
		inputString = inputString.trim();
		String[] words = inputString.split(" ");
		
		//3.Set Table
		table = words[2].toLowerCase();
		
		if (words.length == 6){
			String keys = words[3].replace('(', ' ');
			keys = keys.replace(')', ' ');
			keys = keys.trim();
			String[] keyslist = keys.split(",");
			String values = words[5].replace('(', ' ');
			values = values.replace(')', ' ');
			values = values.trim();
			String[] valueList = values.split(",");
			for(int idx = 0; idx < valueList.length; idx++) 
			{
				DMLSetAttributeValues.put(keyslist[idx].toLowerCase(), valueList[idx]);
			}		
		}
		else{
			// 4. set attributes Values
			String values = words[4].replace('(', ' ');
			values = values.replace(')', ' ');
			values = values.trim();
			String[] valueList = values.split(",");
			for(int idx = 0; idx < valueList.length; idx++)
			{
				DMLSetAttributeValues.put(MySqlSchemaParser.TableAttrs.get(table).get(idx).toString(), valueList[idx]);
			}			
		}	
	}

	
	@Override
	public void SetPrimaryKeyValue() {
		PKValue = "";
		List<String> PKAttributes = MySqlSchemaParser.TablePkeys.get(table);	
		for(String attribute: PKAttributes)
		{
			PKValue += DMLSetAttributeValues.get(attribute) + ";";
		}
		PKValue = PKValue.trim();
	}

	
	@Override
	public Boolean isTableLevelFence() {
		return false;
	}

	
	@Override
	public Boolean isRecordLevelFence() {
		return false;
	}

	@Override
	public void SetForeignKeyValues()
	{
		List<Fkey> FKeys= MySqlSchemaParser.TableFkeys.get(table);
		for(Fkey fk: FKeys)
		{
			String pk_table = fk.getPk_table();
			List<String> columns = fk.getFk_cols();
			String keyValue = "";
			Boolean keyFound = true;
			for(String col: columns)
			{
				String val = DMLSetAttributeValues.get(col);
				if (val == null)
				{
					keyFound = false;
					break;
				}
				keyValue = keyValue + val +";";
			}
			if (keyFound == true)
			{
				FKValue fKValue = new FKValue(keyValue,pk_table);
				FKValues.add(fKValue);
			}
		}
	}
	
	@Override
	public String toDMLString() {
		String attributes = "(";
		String values = "(";
		for(Map.Entry<String, String> entry :DMLSetAttributeValues.entrySet())
		{
			attributes = attributes + entry.getKey() + ",";
			values = values + entry.getValue() + ",";
		}
		attributes = attributes.substring(0, attributes.length() - 1) + ")";
		values = values.substring(0, values.length() - 1) + ")";
		DMLString = "insert into " + table + attributes + " values " + values;
		return DMLString;
	}
	
}
