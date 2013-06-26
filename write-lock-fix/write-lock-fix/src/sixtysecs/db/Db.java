package sixtysecs.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class Db {

	private Db() {
		throw new IllegalStateException(
				"Class cannot be constructed. Only contains static helper methods.");
	}

	/**
	 * See {@link Statement#executeQuery(String) } for details
	 * 
	 * @throws SQLException
	 */
	public static ResultSet executeQuery(Connection con, String cmd)
			throws SQLException {

		System.out.println(cmd);
		ResultSet rs = null;
		Statement stmt = null;
		stmt = con.createStatement();
		rs = stmt.executeQuery(cmd);
		return rs;

	}

	/**
	 * See {@link Statement#execute(String) } for details
	 * 
	 * @throws SQLException
	 */
	protected static boolean execute(Connection con, String cmd)
			throws SQLException {
		Statement stmt = null;

		System.out.println(cmd);
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

	public static String str(String val) throws SQLException {
		if (val == null || val.equals("")) {
			throw new SQLException("this value " + val + " cannot be empty");
		}

		return "'" + apostrophe(val) + "'";
	}

	private static String apostrophe(String val) {
		if (val != null) {
			val = val.replaceAll("'", "''");
		}
		return val;
	}
}
