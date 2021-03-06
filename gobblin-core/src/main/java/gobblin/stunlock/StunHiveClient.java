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
				LOG.debug("JDBC Connection is Valid.");
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

		// stmt.close();
		// con.close();
	}

	private static void TryQuery(Statement statement, String query) throws SQLException {
		int retryCount = 20;
		Throwable lastError = null;
		for (int i = 0; i <= retryCount; i++) {
			try {
				LOG.debug("EXECUTE: " + query);
				if (statement.execute(query)) {
					ResultSet res = statement.getResultSet();

					LOG.debug("EXECUTE QUERY DONE, RESULTS:");
					while (res.next())
						LOG.info(res.getString(1));
				}

				if (statement.getWarnings() != null)
					LOG.warn(statement.getWarnings().getMessage());
				
				if(lastError != null)
					LOG.info("StunHive.TryQyery succeeded after " + i + "retries. Rejoice!");

				LOG.debug("EXECUTE QUERY FULLY DONE");
				return;
			} catch (Throwable e) {
				LOG.warn("StunHive.TryQyery threw with " + (retryCount - i) + " retries left on query " + query + ". Sleeping 10s. Exception: " + e.toString());
				lastError = e;
				try {
					Thread.sleep(10000);
				} catch(InterruptedException ex) {
				    Thread.currentThread().interrupt();
				}
			}
		}
		LOG.error("StunHive.TryQyery FAILED after max retries ("+  retryCount + ")\n* Query: " + query + "\n* Last Exception: " + lastError);
		throw new SQLException("StunHive.TryQyery FAILED after max retries ("+  retryCount + ")\n* Query: " + query + "\n* Last Exception: " + lastError, lastError);
	}
}