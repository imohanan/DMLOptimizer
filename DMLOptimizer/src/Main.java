
public class Main {

	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MySqlSchemaParser schemaParser = new MySqlSchemaParser();
		String DBName= "bg";
		schemaParser.findTablesInDB(DBName);
		System.out.println(schemaParser.TablesInDB);
		
	}

}
