package model;

import java.util.List;
import java.util.Map;

import main.MySqlSchemaParser;

public class UpdateDML extends DML{

	public UpdateDML(String inputString) {
		// 1. Set DMLString		
		inputString = inputString.trim();
		if(inputString.endsWith(";"))
		{
			inputString = inputString.substring(0,inputString.length() - 1);
		}
		inputString = inputString.trim();
		DMLString = inputString;			
		// 2. Set type
		type = DMLType.UPDATE;
		String[] words = inputString.split("\\s*(?i) where \\s*");
		String[] setClause = words[0].split("\\s*(?i) set \\s*");	
		// 3. set table
		String generateTable = setClause[0].replaceFirst("\\s*(?i)update\\s*", "").trim();
		table = generateTable.toLowerCase().trim();
		// 4. set conditions
		String[] clauses = words[1].split("\\s*(?i) and \\s*");
		for (String eachClause: clauses)
		{
			String [] elements = eachClause.split("=");
			DMLGetAttributeValues.put(elements[0].trim().toLowerCase(), elements[1].trim());			
		}
		// 5. set attributes Values
		String [] indClause = setClause[1].split(",");
		for (String indSetClause : indClause)
		{
			String[] elements = indSetClause.split("=");
			DMLSetAttributeValues.put(elements[0].trim().toLowerCase(), elements[1].trim());
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
		String setClause = "";
		String whereClause = "";
		for(Map.Entry<String, String> entry :DMLSetAttributeValues.entrySet())
		{
			setClause = setClause + entry.getKey() +"=" + entry.getValue()+ ",";
		}
		setClause = setClause.substring(0, setClause.length() - 1);
		for(Map.Entry<String, String> entry: DMLGetAttributeValues.entrySet() )
		{
			whereClause = whereClause + entry.getKey() + "=" + entry.getValue() + " and ";
		}
		whereClause = whereClause.substring(0, whereClause.length() - 5);
		DMLString = "update " + table + " set " + setClause + " where " + whereClause;
		return DMLString;
	}

}

