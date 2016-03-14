import java.util.HashMap;

public class DML {

	public String DmlString;
	public String table;
	public DMLType type;
	public HashMap<String,String> attributeValues;
	// public PK_Values;
	// public FK_Values;
	public String PKMap_Id;
	
	public DML(String inputString)
	{
		
	}
	
	public Boolean IsRecordLevelFence()
	{
		// ToDo
		return false;
	}
	
	public Boolean IsTableLevelFence()
	{
		//ToDo
		return false;
	}
	
	public void ChangeValues(HashMap<String, String> newAttributes, DMLType type)
	{
		// ToDo
	}
	
}

