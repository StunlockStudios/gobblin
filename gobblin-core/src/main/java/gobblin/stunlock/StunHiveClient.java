package gobblin.stunlock;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;

public class StunHiveClient {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static final Logger LOG = LoggerFactory.getLogger(StunHiveClient.class);

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void ExecuteStatements(String hiveUrl, String user, String password, String... queries)
			throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			throw new SQLException("ClassNotFoundException Failed to initialize class " + driverName, e);
		} catch (LinkageError e) {
			throw new SQLException("LinkageError Failed to initialize class " + driverName, e);
		}

		// replace "hive" here with the name of the user the queries should run
		// as
		// Connection con =
		// DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
		// "hive", "");

		Connection con = DriverManager.getConnection(hiveUrl, user, password);
		try {
			if (con != null && con.isClosed() == false)
				LOG.info("JDBC Connection is Valid.");
			else
				LOG.error("JDBC Connection IS INVALID! :(");
		} catch (SQLException e) {
			LOG.error("JDBC Connection SQLException on con.isValid! " + e.toString());
			throw new SQLException("Failed to initialize class " + driverName, e);
		}
		Statement stmt = con.createStatement();

		//// See if the database connection and the actual database is valid
		// TryQuery(stmt, "SHOW DATABASES");
		// TryQuery(stmt, "USE default");
		// TryQuery(stmt, "SHOW TABLES");
		// TryQuery(stmt, "DESCRIBE metrics_histograms_25");

		for (String query : queries) {
			TryQuery(stmt, query);
		}

		// Ska inte dessa köras?
		// stmt.close();
		// con.close();
	}

	private static void TryQuery(Statement statement, String query) throws SQLException {
		LOG.info("EXECUTE: " + query);
		if (statement.execute(query)) {
			ResultSet res = statement.getResultSet();

			LOG.info("EXECUTE QUERY DONE, RESULTS:");
			while (res.next())
				LOG.info(res.getString(1));
		}

		if (statement.getWarnings() != null)
			LOG.warn(statement.getWarnings().getMessage());

		LOG.info("EXECUTE QUERY FULLY DONE");
	}
}