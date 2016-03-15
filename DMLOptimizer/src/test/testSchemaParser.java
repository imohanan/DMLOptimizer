package test;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import main.MySqlSchemaParser;
import model.Fkey;
public class testSchemaParser {
	
		public static void main(String[] args) throws SQLException {
//			// TODO Auto-generated method stub
//			List<Fkey> fkeys=new LinkedList<Fkey>();
//			MySqlSchemaParser.setupConnection("bg");
//			fkeys=MySqlSchemaParser.getFKey("manipulation");
//			for (Fkey fk:fkeys){
//			System.out.println(fk.fkToString());
//			}
//
//			
			MySqlSchemaParser.init_Schema(args[0],args[1],args[2]);
	}

}
