package util;

public class Util {

	public static String[] SplitDMLStrings(String dmlString)
	{
		dmlString = dmlString.replace(";", " ");
		dmlString = dmlString.trim();
		String[] parts = dmlString.split(" WHERE ");
		if (parts.length == 1)
			return new String[]{dmlString};
		
		String DMLPre = parts[0];
		String[] DMLPosts = parts[1].split(" OR ");
		
		String NewDMLs[] = new String[DMLPosts.length];
		
		for(int idx = 0; idx < DMLPosts.length; idx++)
		{
			String whereClause = DMLPosts[idx];
			String newDML = DMLPre.trim() + " WHERE " +  whereClause.trim() +";";
			NewDMLs[idx] = newDML; 
		}
		
		return NewDMLs;
	}
}
