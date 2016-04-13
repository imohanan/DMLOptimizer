package util;

import java.util.List;
import java.util.Map;

import main.MySqlSchemaParser;
import model.DML;

public class ManualBatching {

	public static String batchInsert(List<DML> DMLsToBatch) {
		String table = DMLsToBatch.get(0).table;
		
		String attributes = "(";
		for(String attr: MySqlSchemaParser.TableAttrs.get(table))
		{
			attributes = attributes + attr + ",";
		}
		attributes = attributes.substring(0, attributes.length() - 1) + ")";
		
		String batchedValues = "";
		for(DML dml: DMLsToBatch)
		{
			String values = "(";
			for(String attr: MySqlSchemaParser.TableAttrs.get(table))
			{
				String attrValue = dml.DMLSetAttributeValues.get(attr);
				if (attrValue != null)
					values = values + attrValue + ",";
				else
					values = values + MySqlSchemaParser.AttrInitVal.get(table).get(attr) + ",";
			}
			values = values.substring(0, values.length() - 1) + "),";
			batchedValues = batchedValues + values;
		}
		batchedValues = batchedValues.substring(0, batchedValues.length() - 1);
		
		String batchedDML = "insert into " + table + attributes + " values " + batchedValues;
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
