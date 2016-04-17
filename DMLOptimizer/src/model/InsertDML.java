package model;

import java.util.List;
import java.util.Map;

import main.MySqlSchemaParser;

public class InsertDML extends DML{

	public InsertDML(String inputString) {
		//1. set String
		inputString = inputString.trim();
		if (inputString.charAt(inputString.length() - 1) == ';')
			inputString = inputString.substring(0, inputString.length() - 1);
		DMLString = inputString;

		// 2. Set Type
				type = DMLType.INSERT;
				
		String[] words = inputString.split("\\s*(?i) values\\s*");
		String tableName = words[0].split("\\s*(?i) into\\s*")[1];
		
		if (tableName.indexOf('(') == -1) //insert into table values(..,..,)
		{
			table = tableName.trim().toLowerCase();
			// 4. set attributes Values
			String values = words[1];
			if (values.charAt(values.length() - 1) == ')')
				values = values.substring(0, values.length() - 1);
			if (values.charAt(0) == '(')
				values = values.substring(1, values.length());
			values = values.trim();
			String[] valueList = values.split(",");
			for(int idx = 0; idx < valueList.length; idx++)
			{
				DMLSetAttributeValues.put(MySqlSchemaParser.TableAttrs.get(table).get(idx).toString(), valueList[idx].trim());
			}				
		}
		else //insert into table(key1, key2) values(val1, val2)
		{
			String[] tableNameSplit = tableName.split("\\s*(?i) \\(\\s*");
			table = tableNameSplit[0].trim().toLowerCase();
			String keys = tableNameSplit[1].replace('(', ' ');
			keys = keys.replace(')', ' ');
			keys = keys.trim();
			String[] keyslist = keys.split(",");
			
			String values = words[1];
			if (values.charAt(values.length() - 1) == ')')
				values = values.substring(0, values.length() - 1);
			if (values.charAt(0) == '(')
				values = values.substring(1, values.length());
			values = values.trim();
			String[] valueList = values.split(",");

			for(int idx = 0; idx < valueList.length; idx++)
			{
				DMLSetAttributeValues.put(keyslist[idx].trim().toLowerCase(), valueList[idx].trim());
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
