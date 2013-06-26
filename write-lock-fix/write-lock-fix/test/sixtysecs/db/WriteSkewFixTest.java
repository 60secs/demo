package sixtysecs.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;

import org.junit.Ignore;
import org.junit.Test;

public class WriteSkewFixTest {
	private final static String connectionString = "jdbc:sqlserver://127.0.0.1;databaseName=write_skew;user=demo;password=demo;";
	private final static String existingTableName = "foo";

	private final static KeyedSerializedConnectionFactory writeSkewFix;

	static {
		KeyedSerializedConnectionFactory tmpWriteSkewFix = null;
		try {
			tmpWriteSkewFix = new KeyedSerializedConnectionFactory(
					connectionString, existingTableName);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		writeSkewFix = tmpWriteSkewFix;
	}

	@Ignore
	@Test
	public void expectCommitDoesNotThrowsSqlExceptionIfConnectionNotDropped()
			throws Exception {
		String lockName = "lock1234";

		Connection transaction = null;
		try {
			transaction = writeSkewFix.getKeyedSerializedConnection(lockName);
			transaction.commit();
		} catch (Exception e) {
			if (transaction != null) {
				transaction.rollback();
			}
			throw e;
		}
	}

	@Test
	public void expectOneTransactionHasNoErrors() throws Exception {
		Connection con = null;
		String lockName = "lock12345";

		try {
			con = writeSkewFix.getKeyedSerializedConnection(lockName);
		} finally {
			if (con != null) {
				con.rollback();
			}
		}
	}

	@Test(expected = SQLRecoverableException.class)
	public void expectSecondTransactionForLockThrowsSQLRecoverableException()
			throws Exception {
		Connection transaction = null;
		Connection transaction2 = null;

		String lockName = "lock123456";

		try {
			transaction = writeSkewFix.getKeyedSerializedConnection(lockName);
			transaction2 = writeSkewFix.getKeyedSerializedConnection(lockName);
		} finally {
			if (transaction != null) {
				transaction.rollback();
			}
			if (transaction2 != null) {
				transaction.rollback();
			}

		}
	}

}
