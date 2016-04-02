package model;

import java.util.List;
import java.util.Map;

import main.MySqlSchemaParser;

public class UpdateDML extends DML{

	public UpdateDML(String inputString) {
		// 1. Set DMLString
		DMLString = inputString;
		
		inputString = inputString.replace(';', ' ');
		inputString = inputString.trim();
		String[] words = inputString.split(" ");
		
		// 2. Set type
		type = DMLType.UPDATE;
		
		// 3. set table
		table = words[1];
		
		// 4. set attributes Values
		String[] clauses = inputString.split(" where ");
		
		String[] setClauses = clauses[0].split(" set ");
		String[] setAttValues = setClauses[1].split(",");
		for(String setAttVal: setAttValues)
		{
			String[] elements = setAttVal.trim().split("=");
			DMLSetAttributeValues.put(elements[0], elements[1].toString());
		}
		
		String[] attVals = clauses[1].split(" and ");
		for(String attVal: attVals)
		{
			String[] elements = attVal.split("=");
			DMLGetAttributeValues.put(elements[0], elements[1].toString());
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
		DMLString = "update " + table + " set " + setClause + " where " + whereClause + ";";
		return DMLString;
	}

}

