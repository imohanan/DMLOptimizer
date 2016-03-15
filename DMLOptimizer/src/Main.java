import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;


public class Main {

	
	
	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
//String result="";
		List<Fkey> fkeys=new LinkedList<Fkey>();
		//List<String> result=new LinkedList<String>();
		MySqlSchemaParser.setupConnection("bg");
		//result=MySqlSchemaParser.GetAttrType("friendship", "inviteeID");
		//result=MySqlSchemaParser.getAllTables("bg");
		fkeys=MySqlSchemaParser.getFKey("friendship");
		for (Fkey fk:fkeys){
		System.out.println(fk.fkToString());
		}
//		for(String st:result){
//			System.out.println(st);
//		}
//		System.out.println(result);
		//System.out.println(result);
//		String DBName= "bg";
//		schemaParser.findTablesInDB(DBName);
//		System.out.println(schemaParser.TablesInDB);
		
	}

}
