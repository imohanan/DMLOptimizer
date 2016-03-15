package main;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import util.Util;

public class Main {

	
	
	public static void main(String[] args) {

		MySqlSchemaParser schemaParser = new MySqlSchemaParser();
		
		
		// TODO Auto-generated method stub
		Path filePath = Paths.get("./LogFiles/dmls_log.txt");
		Charset charset = Charset.forName("US-ASCII");
		try (BufferedReader reader = Files.newBufferedReader(filePath, charset)) {
		    String line = null;
		    while ((line = reader.readLine()) != null) {
		    	
		    	String[] DMLsArray = Util.SplitDMLStrings(line);
		    	for(String singleDMLLine: DMLsArray)
		    	{
		    		DML dml = new DML(singleDMLLine);
			    	if (dml.IsTableLevelFence)
			        {
			        	//TODO
			        	System.out.println("Table Level Fence found");
			        }
			        else if (dml.IsRecordLevelFence)
			        {
			        	//TODO
			        	System.out.println("Record Level Fence found");
			        }
			        else
			        {
			        	dml.SetPrimaryKeyValue();
			        	dml.toDMLString();
			        }
		    	}
		        
		    }
		} catch (IOException x) {
		    System.err.format("IOException: %s%n", x);
		}
		
	}

}
