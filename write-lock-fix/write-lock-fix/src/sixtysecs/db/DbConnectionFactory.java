package sixtysecs.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

/**
 * A factory for obtaining connections to the database. Auto-commit connections
 * are implemented as a singleton connection. Non-autocommit (transactional)
 * connections are each different connections.
 * 
 * @author Eric Driggs
 */
public class DbConnectionFactory {

	protected static Logger logger = Logger.getRootLogger();

	private String connectionString;
	Connection singletonConnection;

	/**
	 * Construct a new factory and initializes singleton auto-commit connection
	 * based on connection string.
	 * 
	 * @param connectionString
	 * @throws SQLException
	 *             if unable to initalize auto-commit singleton connection
	 */
	DbConnectionFactory(String connectionString) throws SQLException {
		this.connectionString = connectionString;
		this.singletonConnection = getNewConnection(true);
	}

	/**
	 * 
	 * @return a new transactional connection, which requires a commit or
	 *         rollback
	 * @throws SQLException
	 *             if unable to initialize the connection
	 */
	Connection getNewTransactionConnection() throws SQLException {
		return getNewConnection(false);
	}

	/**
	 * @return singleton auto-commit transaction
	 */
	public Connection getSingletonConnection() {
		return singletonConnection;
	}

	private Connection getNewConnection(boolean autoCommit) throws SQLException {
		Connection con = null;

		con = DriverManager.getConnection(connectionString);
		con.setAutoCommit(autoCommit);
		return con;

	}
}
