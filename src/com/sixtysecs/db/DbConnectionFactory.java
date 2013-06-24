package com.sixtysecs.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;



/**
 * TODO: contract
 * 
 * @author Eric Driggs
 */
public class DbConnectionFactory {

    protected static Logger logger = Logger.getRootLogger();
    
	private String connectionString;
	Connection singletonConnection;

	public DbConnectionFactory(String connectionString, boolean autoCommit)
			throws SQLException {
		this.connectionString = connectionString;
		//TODO: implement as lazy init singleton
		this.singletonConnection = getNewConnection(true);
	}

	public Connection getNewTransactionConnection() {
		return getNewConnection(false);
	}
	
	/**
	 * @return singleton auto-commit transaction
	 */
	public Connection getSingletonConnection() {
		return singletonConnection;
	}

	private Connection getNewConnection(boolean autoCommit) {
		Connection con = null;
		try {
			con = DriverManager.getConnection(connectionString);
			con.setAutoCommit(autoCommit);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return con;

	}

	/**
	 * Closes the ResultSet provided and its associated statement.
	 * 
	 * @param rs
	 *            the ResultSet to close
	 * @return Returns true if able to close the ResultSet and no SQLException
	 *         occurred.<br>
	 *         Returns false if nothing to close or a SQLException occurred when
	 *         closeing the ResultSet or Statement.
	 */
	public static boolean closeResultSetAndStatement(ResultSet rs) {
		if (rs == null) {
			return false;
		}
		try {
			Statement stmt = rs.getStatement();
			if (stmt != null) {
				stmt.close();
			}
			rs.close();
		} catch (SQLException e) {
			System.err.println("Error in closing ResultSet and Statement: "
					+ e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * See {@link Statement#execute(String) } for details
	 * 
	 * @throws SQLException
	 */
	protected static boolean execute(Connection con, String cmd)
			throws SQLException {
		Statement stmt = null;

		try {
			stmt = con.createStatement();
			stmt.setEscapeProcessing(false);
			boolean bRes = stmt.execute(cmd);
			return bRes;
		} finally {
			if (stmt != null) {
				stmt.close();
			}
		}
	}

	/**
	 * See {@link Statement#executeQuery(String) } for details
	 * 
	 * @throws SQLException
	 */
	public static ResultSet executeQuery(Connection con, String cmd)
			throws SQLException {

		ResultSet rs = null;
		Statement stmt = null;
		stmt = con.createStatement();
		rs = stmt.executeQuery(cmd);
		return rs;

	}
}
