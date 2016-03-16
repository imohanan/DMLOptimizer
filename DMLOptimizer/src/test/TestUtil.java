package test;

import util.Util;

public class TestUtil {
	
	public static void main(String[] args)
	{
		String testDML ="SELECT * FROM USERS WHERE USERID=12 AND LOCATION=\"la\" OR USERID=24 OR USERID=30 OR USERID=40;";
		testDML = testDML.toLowerCase();
		String[] DMLs = Util.SplitDMLStrings(testDML);
		
		for(String DML: DMLs)
			System.out.println(DML);
	}

}
