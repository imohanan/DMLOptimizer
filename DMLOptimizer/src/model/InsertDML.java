package model;

import java.util.List;
import java.util.Map;

import main.MySqlSchemaParser;

public class InsertDML extends DML{

	public InsertDML(String inputString) {
		//1. set String
		DMLString = inputString.toLowerCase();
		// 2. Set Type
		type = DMLType.INSERT;
		
		inputString = inputString.replace(';', ' ');
		inputString = inputString.trim();
		String[] words = inputString.split(" ");
		
		//3.Set Table
		table = words[2];
		
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
	public void toDMLString() {
		String attributes = "(";
		String values = "(";
		for(Map.Entry<String, String> entry :DMLSetAttributeValues.entrySet())
		{
			attributes = attributes + entry.getKey() + ",";
			values = values + entry.getValue() + ",";
		}
		attributes = attributes.substring(0, attributes.length() - 1) + ")";
		values = values.substring(0, values.length() - 1) + ")";
		DMLString = "insert into " + table + attributes + " values " + values + ";";
	}
	
}
