package util;

import java.util.List;
import java.util.Map;

import model.DML;

public class ManualBatching {

	public static String batchInsert(List<DML> DMLsToBatch) {
		String table = DMLsToBatch.get(0).table;
		
		String batchedDML = "";//"insert into " + table + attributes + " values " + values;
		return batchedDML;
	}

	public static String batchDelete(List<DML> DMLsToBatch) {
		String table = DMLsToBatch.get(0).table;
		String batchWhereClauses = "";
		for(DML dml:DMLsToBatch)
		{
			String whereClause = "(";
			for(Map.Entry<String, String> entry: dml.DMLGetAttributeValues.entrySet() )
			{
				whereClause = whereClause + entry.getKey() + "=" + entry.getValue() + " and ";
			}
			whereClause = whereClause.substring(0, whereClause.length() - 5) + ")" ;
			batchWhereClauses = batchWhereClauses + whereClause + " or ";
		}
		batchWhereClauses = batchWhereClauses.substring(0, batchWhereClauses.length() - 4);
		String batchedDML = "delete from " + table + " where " + batchWhereClauses;
		return batchedDML;
	}

}
