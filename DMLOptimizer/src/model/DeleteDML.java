package model;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import main.MySqlSchemaParser;

public class DeleteDML extends DML {

	public DeleteDML(String inputString) {
		// 1. set DMLString
		DMLString = inputString;
		inputString = inputString.replace(';', ' ');
		inputString = inputString.trim();
		String[] words = inputString.split(" ");
		
		// 2. Set type
		type = DMLType.DELETE;
		
		// 3. set table
		table = words[2];
		
		// 4. set attributes Values
		String[] clauses = inputString.split(" WHERE ");
		String[] attVals = clauses[1].split(" AND ");
		for(String attVal: attVals)
		{
			String[] elements = attVal.split("=");
			DMLGetAttributeValues.put(elements[0].toString(), elements[1].toString());
		}
	}

	
	@Override
	public void SetPrimaryKeyValue() {
		PKValue = "";
		Vector<String> PKAttributes = MySqlSchemaParser.TablePkeys.get(table);
		for(int idx=0; idx < PKAttributes.size(); idx++)
		{
			PKValue += DMLGetAttributeValues.get(PKAttributes.get(idx)) + ";";
		}
	}


	@Override
	public void toDMLString() {
		String whereClause = "";
		for(Map.Entry<String, String> entry: DMLGetAttributeValues.entrySet() )
		{
			whereClause = whereClause + entry.getKey() + "=" + entry.getValue() + " AND ";
		}
		whereClause = whereClause.substring(0, whereClause.length() - 5);
		DMLString = "DELETE FROM " + table + " WHERE " + whereClause + ";";
		
	}

}
