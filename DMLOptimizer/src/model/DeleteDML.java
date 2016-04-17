package model;

import java.util.List;
import java.util.Map;

import main.MySqlSchemaParser;

public class DeleteDML extends DML {

	public DeleteDML(String inputString) {
		// 1. set DMLString
		inputString = inputString.trim();
		if(inputString.endsWith(";"))
		{
			inputString = inputString.substring(0,inputString.length() - 1);
		}
		inputString = inputString.trim();
		DMLString = inputString;		
		// 2. Set type
		type = DMLType.DELETE;
		String[] words = inputString.split("\\s*(?i) where \\s*");
		String[] generateTable = words[0].split("\\s*(?i) from \\s*");
		// 3. set table
		table = generateTable[1].trim().toLowerCase();		
		// 4. set attributes Values
		String[] clauses = words[1].split("\\s*(?i) and \\s*");
		for (String eachClause: clauses)
		{
			String [] elements = eachClause.split("=");
			DMLGetAttributeValues.put(elements[0].trim().toLowerCase(), elements[1].trim());
			
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
		PKValue = PKValue.trim();
	}


	@Override
	public String toDMLString() {
		String whereClause = "";
		for(Map.Entry<String, String> entry: DMLGetAttributeValues.entrySet() )
		{
			whereClause = whereClause + entry.getKey() + "=" + entry.getValue() + " and ";
		}
		whereClause = whereClause.substring(0, whereClause.length() - 5);
		DMLString = "delete from " + table + " where " + whereClause;
		return DMLString;
	}

}
