package sixtysecs.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;

import org.junit.Test;

public class KeyedSerializedConnectionFactoryTest {
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

	@Test
	public void expectCommitDoesNotThrow() throws Exception {
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
