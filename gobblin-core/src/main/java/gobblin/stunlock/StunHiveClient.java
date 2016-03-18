package gobblin.stunlock;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class StunHiveClient
{
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void ExecuteStatements(String hiveUrl, String user, String password, String... statements) throws SQLException
	{
		try
		{
			Class.forName(driverName);
		}
		catch (ClassNotFoundException e)
		{
			throw new SQLException("Failed to initialize class " + driverName, e);
		}
		
		// replace "hive" here with the name of the user the queries should run
		// as
		//Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
		
		Connection con = DriverManager.getConnection(hiveUrl, user, password);
		Statement stmt = con.createStatement();
		for(String statement : statements)
		{
			stmt.execute(statement);
		}
		// Ska inte dessa köras?
		//stmt.close();
		//con.close();
	}
}