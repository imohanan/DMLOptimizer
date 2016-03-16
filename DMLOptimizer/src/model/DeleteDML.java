package model;

import java.util.List;
import java.util.Map;

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
		String[] clauses = inputString.split(" where ");
		String[] attVals = clauses[1].split(" and ");
		for(String attVal: attVals)
		{
			String[] elements = attVal.split("=");
			DMLGetAttributeValues.put(elements[0].toString(), elements[1].toString());
		}
	}

	
	@Override
	public void SetPrimaryKeyValue() {
		PKValue = "";
		List<String> PKAttributes = MySqlSchemaParser.TablePkeys.get(table);
		for(String attribute: PKAttributes)
		{
			PKValue += DMLGetAttributeValues.get(attribute) + ";";
		}
	}


	@Override
	public void toDMLString() {
		String whereClause = "";
		for(Map.Entry<String, String> entry: DMLGetAttributeValues.entrySet() )
		{
			whereClause = whereClause + entry.getKey() + "=" + entry.getValue() + " and ";
		}
		whereClause = whereClause.substring(0, whereClause.length() - 5);
		DMLString = "delete from " + table + " where " + whereClause + ";";
		
	}

}
