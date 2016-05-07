package util;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import com.mysql.jdbc.Statement;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

import main.Combiner;
import main.MySqlSchemaParser;
import model.DML;
import model.DMLQueue;
import model.DMLType;

public class Util {
	private static Statement statement = null;
	private static String template = null;
	private static java.sql.PreparedStatement preparedStatement = null;
	private static DMLType currType = null;
	private static DML currDML = null;
	private static String currTable = null;
	public static int batchSize;

	public static String preprocessDMLString(String dmlLine) {
		dmlLine = dmlLine.replace("=", " = ");
		dmlLine = dmlLine.replace("(", " (");
		dmlLine = dmlLine.replace(")", ") ");
		dmlLine = dmlLine.replaceAll("( )+", " ");
		dmlLine = dmlLine.replaceAll("\\s+(?=[^()]*\\))", "");
		return dmlLine;
	}

	public static String[] splitDMLsByOR(String dmlString) {
		dmlString = dmlString.replace(";", " ");
		dmlString = dmlString.trim();
		String[] parts = dmlString.split("\\s*(?i)where\\s*");
		if (parts.length == 1)
			return new String[] { dmlString };

		String DMLPre = parts[0];
		String[] DMLPosts = parts[1].split("\\s*(?i)or\\s*");

		String NewDMLs[] = new String[DMLPosts.length];

		for (int idx = 0; idx < DMLPosts.length; idx++) {
			String whereClause = DMLPosts[idx];
			String newDML = DMLPre.trim() + " where " + whereClause.trim() + ";";
			NewDMLs[idx] = newDML;
		}

		return NewDMLs;
	}
}
