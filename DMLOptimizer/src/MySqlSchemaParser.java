import java.util.HashMap;
import java.util.Vector;

public class MySqlSchemaParser {

	public static HashMap<String,Vector> TableAttrs = new HashMap();
	public static HashMap<String,Integer> AttrType = new HashMap();
	public static HashMap<String,String> AttrTypeString = new HashMap();
	public static HashMap<String,Vector> TablePks = new HashMap();//<TableName,Vector of attributes>
	//  public static HashMap<String,FK_Struct> TableFks = new HashMap();
	public static HashMap<String,String> ImpactedTables = new HashMap();
	public static HashMap<String,String> TablesInDB = new HashMap();
	
	//ToDo remove this class, replace it with the class that gets the values
	
	public void findTablesInDB(String DBName)
	{
		
		
	}
	
	
	
}