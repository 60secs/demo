package sixtysecs.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;

import org.junit.Test;

public class WriteSkewFixTest {

	private final static String connectionString = ""; // TODO:
	private final static String lockName = "lock12345";
	private final static KeyedSerializedConnectionFactory writeSkewFix;

	static {
		KeyedSerializedConnectionFactory tmpWriteSkewFix = null;
		try {
			tmpWriteSkewFix = new KeyedSerializedConnectionFactory(
					connectionString);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		writeSkewFix = tmpWriteSkewFix;
	}

	@Test
	public void testNewKeyedLockTransaction() throws Exception {
		Connection transaction = null;

		try {

			transaction = writeSkewFix.getKeyedSerializedConnection(lockName);
		} finally {
			if (transaction != null) {
				transaction.rollback();
			}
		}
	}

	@Test(expected = SQLRecoverableException.class)
	public void testNewKeyedLockTransactionConcurrency() throws Exception {
		Connection transaction = null;
		Connection transaction2 = null;

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
