package model;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

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
		String[] clauses = inputString.split(" WHERE ");
		//TODO add the SET attributes SET PendCount=PendCount+1?? Should be record level fence?????
		
		String[] setClauses = clauses[0].split(" SET ");
		String[] setAttValues = setClauses[1].split(",");
		for(String setAttVal: setAttValues)
		{
			String[] elements = setAttVal.trim().split("=");
			DMLSetAttributeValues.put(elements[0], elements[1].toString());
		}
		
		String[] attVals = clauses[1].split(" AND ");
		for(String attVal: attVals)
		{
			String[] elements = attVal.split("=");
			DMLGetAttributeValues.put(elements[0], elements[1].toString());
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
		String setClause = "";
		String whereClause = "";
		for(Map.Entry<String, String> entry :DMLSetAttributeValues.entrySet())
		{
			setClause = setClause + entry.getKey() +"=" + entry.getValue()+ ",";
		}
		setClause = setClause.substring(0, setClause.length() - 1);
		for(Map.Entry<String, String> entry: DMLGetAttributeValues.entrySet() )
		{
			whereClause = whereClause + entry.getKey() + "=" + entry.getValue() + " AND ";
		}
		whereClause = whereClause.substring(0, whereClause.length() - 5);
		DMLString = "UPDATE " + table + " SET " + setClause + " WHERE " + whereClause + ";";
	}

}
