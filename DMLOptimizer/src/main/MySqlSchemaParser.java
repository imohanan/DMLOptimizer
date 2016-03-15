package main;
import java.util.HashMap;
import java.util.Vector;

public class MySqlSchemaParser {

	public static HashMap<String,Vector> TableAttrs = new HashMap();
	public static HashMap<String,Integer> AttrType = new HashMap();
	public static HashMap<String,String> AttrTypeString = new HashMap();
	public static HashMap<String,Vector> TablePKs = new HashMap();//<TableName,Vector of attributes>
	//  public static HashMap<String,FK_Struct> TableFks = new HashMap();
	public static HashMap<String,String> ImpactedTables = new HashMap();
	public static HashMap<String,String> TablesInDB = new HashMap();
	
	//ToDo remove this class, replace it with the class that gets the values
	
	public MySqlSchemaParser()
	{
		//TODO remove this constructor
		Vector frndshpAttr = new Vector(3);
		frndshpAttr.addElement("inviterID");
		frndshpAttr.addElement("inviteeID");
		frndshpAttr.addElement("STATUS");
		TableAttrs.put("friendship", frndshpAttr);
		
		Vector frndshpPK = new Vector(2);
		frndshpPK.addElement("inviterID");
		frndshpPK.addElement("inviteeID");
		TablePKs.put("friendship", frndshpPK);
		
	}
	
	public void findTablesInDB(String DBName)
	{
		
		
	}
	
	
	
}