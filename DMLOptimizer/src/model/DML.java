package model;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import main.Main;
import main.MySqlSchemaParser;

public abstract class DML {

	public static int counter = 0;
	public int id;
	public String DMLString;
	public String table;
	public DMLType type;
	public Map<String,String> DMLGetAttributeValues = new HashMap<String,String>(); 
	public Map<String,String> DMLSetAttributeValues = new HashMap<String,String>(); 
	public String PKValue;
	public List<FKValue> FKValues = new LinkedList<>(); 
	public Boolean IsRecordLevelFence = false;
	public Boolean IsTableLevelFence = false;
	public DML NextNode = null;
	public DML PrevNode = null;
	
	public DML() {
		id = counter;
		Main.batcher.DMLTotal++;
		counter++;
	}
	
	public void SetPrimaryKeyValue()
	{
		
	}
	
	public void SetForeignKeyValues()
	{
		// TODO: Handle case where Update is updating a FK-> values removed from foreign key map?
		List<Fkey> FKeys= MySqlSchemaParser.TableFkeys.get(table);
		for(Fkey fk: FKeys)
		{
			String pk_table = fk.getPk_table();
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
				FKValue fKValue = new FKValue(keyValue,pk_table);
				FKValues.add(fKValue);
			}
		}
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
		boolean pendcountDmlSeen = false;
		for (Map.Entry<String, String> entry : dMLSetAttributeValues2.entrySet()) {
		    String key = entry.getKey();
		    String value = entry.getValue().toString();
		    String origValue = DMLSetAttributeValues.get(key);
		    //String newNumber= value.replaceAll("[^.0-9]", "");
		    double newNumber;
		    String test2 = value.replaceAll("[^\\d.]+|\\.(?!\\d)", "");
		    if (test2 == "")
		    	newNumber = 0;
		    else 
		    	newNumber = Double.valueOf(test2);
		    
		    if (origValue == null 
		    		|| !value.toLowerCase().contains(key.toLowerCase())
		    		|| !MySqlSchemaParser.numericTypes.contains(MySqlSchemaParser.AttrTypes.get(table).get(key)) ) {
		    	DMLSetAttributeValues.put(key, value);
		    }
		    else if (value.toLowerCase().contains(key.toLowerCase()) && origValue.toLowerCase().contains(key.toLowerCase())){
		    	pendcountDmlSeen = true;
		    	double origNumber = Double.valueOf(origValue.replaceAll("[^\\d.]+|\\.(?!\\d)", ""));	   
		    	String newValue = key;
		    	if (value.indexOf("+") != -1 && origValue.indexOf("+") != -1) {
		    		double ans = newNumber + origNumber;
		    		// TODO check SQL type and convert accordingly
		    		newValue = newValue + "+" + ans;
		    		DMLSetAttributeValues.put(key, newValue);
		    	}
		    	else if (value.indexOf("+") != -1 && origValue.indexOf("-") != -1) {
		    		double ans = ((-1) * origNumber + newNumber);
		    		if (ans < 0) {
		    			newValue = newValue + "-" + Math.abs(ans);
		    		}
		    		else {
		    			newValue = newValue + "+" + Math.abs(ans);
		    		}		    		
		    		DMLSetAttributeValues.put(key, newValue);
		    	}
		    	else if (value.indexOf("-") != -1 && origValue.indexOf("+") != -1) {
		    		double ans = ((-1) * newNumber) + origNumber;
		    		if (ans < 0) {
		    			newValue = newValue + "-" + Math.abs(ans);
		    		}
		    		else {
		    			newValue = newValue + "+" + Math.abs(ans);
		    		}		    		
		    		DMLSetAttributeValues.put(key, newValue);
		    	}
		    	else if (value.indexOf("-") != -1 && origValue.indexOf("-") != -1) {
		    		double ans = newNumber + origNumber;
		    		newValue = newValue + "-" + ans;
		    		DMLSetAttributeValues.put(key, newValue);
		    	}
		    	else if (value.indexOf("*") != -1){
		    		//TODO
		    	}
		    	
		    }
		    else {
		    	pendcountDmlSeen = true;
		    	double origNumber = Double.valueOf(origValue.replaceAll("[^\\d.]+|\\.(?!\\d)", ""));
		    	String newValue = "";
		    	if (value.indexOf("+") != -1) {
		    		double ans = origNumber + newNumber;
		    		if (ans < 0){
		    			newValue = newValue + "-" + Math.abs(ans);
		    		}
		    		else {
		    			newValue = newValue + "+" + Math.abs(ans);
		    		}
		    		DMLSetAttributeValues.put(key, newValue);
		    	}
		    	else if (value.indexOf("-") != -1) {
		    		double ans = origNumber + ((-1)*newNumber);
		    		if (ans < 0){
		    			newValue = newValue + "-" + Math.abs(ans);
		    		}
		    		else {
		    			newValue = newValue + "+" + Math.abs(ans);
		    		}
		    		DMLSetAttributeValues.put(key, newValue);
		    	}
		    	else if (value.indexOf("*") != -1) {
		    		//TODO
		    	}
		    }
		    
		}
		if (pendcountDmlSeen) {
			Main.batcher.pendcountDML++;
		}
		toDMLString();
	}

	public String toDMLString() {
		return null;
		// TODO Auto-generated method stub
		
	}

	
}
