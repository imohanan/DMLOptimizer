package util;

import java.util.List;

import model.DML;

public class Util {

	public static String[] SplitDMLStrings(String dmlString)
	{
		dmlString = dmlString.replace(";", " ");
		dmlString = dmlString.trim();
		String[] parts = dmlString.split(" where ");
		if (parts.length == 1)
			return new String[]{dmlString};
		
		String DMLPre = parts[0];
		String[] DMLPosts = parts[1].split(" or ");
		
		String NewDMLs[] = new String[DMLPosts.length];
		
		for(int idx = 0; idx < DMLPosts.length; idx++)
		{
			String whereClause = DMLPosts[idx];
			String newDML = DMLPre.trim() + " where " +  whereClause.trim() +";";
			NewDMLs[idx] = newDML; 
		}
		
		return NewDMLs;
	}

	public static void BatchAndPush() {
		// TODO Auto-generated method stub
		
	}

	public static void BatchAndPush(List<DML> listOfAffectedDMLs) {
		// TODO Auto-generated method stub
		
	}
}
