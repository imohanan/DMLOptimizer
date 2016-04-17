package test;

import model.DMLType;
import util.Util;

public class TestUtil {
	
	public static void main(String[] args)
	{
		String test = "W_YTD = W_YTD + 45365";
		double f = Double.valueOf(test.replaceAll("[^\\d.]+|\\.(?!\\d)", ""));
		String y = "";
		
		/*String testDML ="SELECT * FROM USERS WHERE USERID=12 AND LOCATION=\"la\" OR USERID=24 OR USERID=30 OR USERID=40;";
		testDML = testDML.toLowerCase();
		String[] DMLs = Util.splitDMLsByOR(testDML);
		
		for(String DML: DMLs)
			System.out.println(DML);*/
		/*String inputString = "Delete from users wheRe abc = 123 and cdf = 'dfr nifhi' and cvf = 456 ;";
		inputString = inputString.trim();
		if(inputString.endsWith(";"))
		{
			inputString = inputString.substring(0,inputString.length() - 1);
		}
		inputString = inputString.trim();
		
		
		//String[] words = inputString.split(" ");
		
		// 2. Set type
		String[] words = inputString.split("\\s*(?i) where \\s*");
		String[] generateTable = words[0].split("\\s*(?i) from \\s*");
		// 3. set table
		String table = generateTable[1].trim().toLowerCase();		
		//table = words[2].toLowerCase();
		
		// 4. set attributes Values
		String[] clauses = words[1].split("\\s*(?i) and \\s*");
		for (String eachClause: clauses)
		{
			String [] elements = eachClause.split("=");
			String key = elements[0].trim();
			String value = elements[1].trim();
			
		}
		*/
		/*
		String inputString = "Update users set 'location state' = 'Los Angeles', name = 123, age ='fhj sjd' whERE cond1 = abd and 'conf 2' = 'ghf jd'  ;  ";
		inputString = inputString.trim();
		if(inputString.endsWith(";"))
		{
			inputString = inputString.substring(0,inputString.length() - 1);
		}
		inputString = inputString.trim();		
	
		String[] words = inputString.split("\\s*(?i) where \\s*");
		String[] setClause = words[0].split("\\s*(?i) set \\s*");	
		// 3. set table
		String generateTable = setClause[0].replaceFirst("\\s*(?i)update\\s*", "").trim();
		
		// 4. set conditions
		String[] clauses = words[1].split("\\s*(?i) and \\s*");
		String key ="";
		String value ="";
		for (String eachClause: clauses)
		{
			String [] elements = eachClause.split("=");
			key = elements[0].trim();
			value = elements[1].trim();
			//DMLGetAttributeValues.put(elements[0].trim().toLowerCase(), elements[1].trim());			
		}
		// 5. set attributes Values
		String [] indClause = setClause[1].split(",");
		for (String indSetClause : indClause)
		{
			String[] elements = indSetClause.split("=");
			key = elements[0].trim();
			value = elements[1].trim();
			//DMLSetAttributeValues.put(elements[0].trim().toLowerCase(), elements[1].trim());
		}
		*/
	}
	

}
