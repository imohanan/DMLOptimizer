import java.util.HashMap;

public class DML {

	public String DMLString;
	public String table;
	public DMLType type;
	public HashMap<String,String> attributeValues;
	// public PK_Values;
	// public FK_Values;
	public String PKValue;
	
	public DML(String inputString)
	{
		
	}
	
	public Boolean isRecordLevelFence()
	{
		// ToDo
		return false;
	}
	
	public Boolean isTableLevelFence()
	{
		//ToDo
		return false;
	}
	
	public void changeValues(HashMap<String, String> newAttributes, DMLType type)
	{
		// ToDo
	}
	
	public void toDMLString()
	{
		//Update DMlString
	}
	
}

