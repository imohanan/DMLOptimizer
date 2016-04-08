package model;

public class FKValue {
	public String FKValueString = "";
	public String Referenced_Table;
	
	public FKValue(String val, String tableName) {
		FKValueString = val;
		Referenced_Table = tableName;
	
	}
}
