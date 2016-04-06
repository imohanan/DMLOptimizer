package model;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import main.MySqlSchemaParser;
import util.Stats;

public abstract class DML {

	public String DMLString;
	public String table;
	public DMLType type;
	public Map<String,String> DMLGetAttributeValues = new HashMap<String,String>(); 
	public Map<String,String> DMLSetAttributeValues = new HashMap<String,String>(); 
	public String PKValue;
	public Boolean IsRecordLevelFence = false;
	public Boolean IsTableLevelFence = false;
	public DML NextNode = null;
	public DML PrevNode = null;
	
	public DML() {
		Stats.DMLTotal++;
	}
	
	public void SetPrimaryKeyValue()
	{
		
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
		    String newNumber= value.replaceAll("[^0-9]", "");
		    
		    if (origValue == null || !value.toLowerCase().contains(key.toLowerCase())) {
		    	DMLSetAttributeValues.put(key, value);
		    }
		    else if (value.toLowerCase().contains(key.toLowerCase()) && origValue.toLowerCase().contains(key.toLowerCase())){
		    	pendcountDmlSeen = true;
		    	String origNumber = origValue.replaceAll("[^0-9]", "");	   
		    	String newValue = key;
		    	if (value.indexOf("+") != -1 && origValue.indexOf("+") != -1) {
		    		int ans = Integer.parseInt(newNumber) + Integer.parseInt(origNumber);
		    		newValue = newValue + "+" + ans;
		    		DMLSetAttributeValues.put(key, newValue);
		    	}
		    	else if (value.indexOf("+") != -1 && origValue.indexOf("-") != -1) {
		    		int ans = ((-1) * Integer.parseInt(newNumber)) + Integer.parseInt(origNumber);
		    		if (ans < 0) {
		    			newValue = newValue + "-" + Math.abs(ans);
		    		}
		    		else {
		    			newValue = newValue + "+" + Math.abs(ans);
		    		}		    		
		    		DMLSetAttributeValues.put(key, newValue);
		    	}
		    	else if (value.indexOf("-") != -1 && origValue.indexOf("+") != -1) {
		    		int ans = ((-1) * Integer.parseInt(newNumber)) + Integer.parseInt(origNumber);
		    		if (ans < 0) {
		    			newValue = newValue + "-" + Math.abs(ans);
		    		}
		    		else {
		    			newValue = newValue + "+" + Math.abs(ans);
		    		}		    		
		    		DMLSetAttributeValues.put(key, newValue);
		    	}
		    	else if (value.indexOf("-") != -1 && origValue.indexOf("-") != -1) {
		    		int ans = Integer.parseInt(newNumber) + Integer.parseInt(origNumber);
		    		newValue = newValue + "-" + ans;
		    		DMLSetAttributeValues.put(key, newValue);
		    	}
		    	else if (value.indexOf("*") != -1){
		    		//TODO
		    	}
		    	
		    }
		    else {
		    	pendcountDmlSeen = true;
		    	String origNumber = origValue;
		    	String newValue = "";
		    	if (value.indexOf("+") != -1) {
		    		int ans = Integer.parseInt(origNumber) + Integer.parseInt(newNumber);
		    		if (ans < 0){
		    			newValue = newValue + "-" + Math.abs(ans);
		    		}
		    		else {
		    			newValue = newValue + "+" + Math.abs(ans);
		    		}
		    		DMLSetAttributeValues.put(key, newValue);
		    	}
		    	else if (value.indexOf("-") != -1) {
		    		int ans = Integer.parseInt(origNumber) + ((-1)*Integer.parseInt(newNumber));
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
			Stats.pendcountDML++;
		}
		
	}

	public String toDMLString() {
		return null;
		// TODO Auto-generated method stub
		
	}

	
}
