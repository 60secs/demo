package sixtysecs.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;

import org.apache.log4j.Logger;

/**
 * Provides blocking connections which are guaranteed to have a unique
 * identifier. This overcomes write skew anomaly limitations of Sql Server's
 * read committed, snapshot isolation.
 * 
 * @author Eric Driggs
 * 
 */
public class KeyedSerializedConnectionFactory {
	final static boolean autoCommit = false;
	final String existingTableName;
	protected static Logger logger = Logger.getRootLogger();
	ConnectionFactory dbConnectionFactory;

	public KeyedSerializedConnectionFactory(String connectionString,
			String existingTableName) throws SQLException {
		this.dbConnectionFactory = new ConnectionFactory(connectionString);
		this.existingTableName = existingTableName;
	}

	/**
	 * Only one transactional connection per key can be run at a time.
	 * Additional attempts to create a connection with the same key at the same
	 * time will either throw or block.
	 * <p>
	 * Should only and always be called if all of the following conditions are
	 * true:
	 * <ul>
	 * <li>It is allowed for the operation about to be performed to be retried
	 * on failure.</li>
	 * <li>A unique identifier is known for the data being modified</li>
	 * </ul>
	 * <p>
	 * 
	 * @param key
	 *            the string key to lock on.
	 * @return a keyed transactional connection
	 * @throws SQLException
	 *             if there is a database error.
	 * @throws SQLRecoverableException
	 *             If there is already a transaction in progress for the key
	 *             string requested
	 */
	public Connection getKeyedSerializedConnection(String lockName)
			throws SQLException, SQLRecoverableException {
		Connection outerLockTransaction = null; // TODO: refactor to
												// outerConnection

		if (lockName == null || lockName.length() == 0
				|| lockName.length() > 250) {
			throw new IllegalArgumentException(
					"lockName length must be between 1 and 250 characters");
		}

		/*
		 * The outer lock can only be taken if both the inner and outer lock are
		 * available. If a transaction is in progress, an attempt to get a
		 * subscriber transaction may either fail or block. <p> By using a
		 * separate transaction for the outer lock, we ensure that if blocking
		 * does occur on the outer lock, the transaction returned will reflect
		 * the state of the database after the outer lock was obtained instead
		 * of the state when the lock was requested.
		 */
		try {
			outerLockTransaction = dbConnectionFactory
					.getNewTransactionConnection();
			performDmlToBeginTransactionContext(outerLockTransaction);

			/*
			 * Checks to see that both inner and outer lock are available,
			 * respectively. If both are available, takes outer lock
			 */
			getKeyedOuterLock(outerLockTransaction, lockName);
		} catch (SQLException ex) {
			if (outerLockTransaction != null) {
				outerLockTransaction.rollback();
			}
			throw ex;
		}

		Connection innerConnection = null; // TODO: refactor to inner
											// connection
		try {
			innerConnection = dbConnectionFactory.getNewTransactionConnection();
			/* ObtainWriteSkewFixk in fresh transaction context we will return */
			performDmlToBeginTransactionContext(innerConnection);
			getKeyedInnerLock(innerConnection, lockName);
		} catch (SQLException ex) {
			if (innerConnection != null) {
				innerConnection.rollback();
			}
			throw ex;
		} finally {
			/*
			 * Ok to release outer lock because attempts to grab outer lock will
			 * fail if inner lock is still being held
			 */
			outerLockTransaction.rollback();
		}
		return innerConnection;
	}

	/**
	 * Need to read from current database before MSSQL will allow sp_getapplock.
	 * If this is not performed, an error will be thrown stating that the
	 * statement must be executed in the context of a transaction.
	 * 
	 * @throws SQLException
	 * @throws SQLRecoverableException
	 */
	private void performDmlToBeginTransactionContext(Connection con)
			throws SQLException, SQLRecoverableException {
		{ // Ensure subscriber exists

			StringBuilder builder = new StringBuilder();
			builder.append(" select top 1 * from " + existingTableName);

			String cmd = builder.toString();
			ResultSet rs = null;
			try {
				rs = Db.executeQuery(con, cmd);
			} finally {
				if (rs != null) {
					Db.closeResultSetAndStatement(rs);
				}
			}
		}
	}

	/**
	 * Should only be called inside a transaction.
	 * <p>
	 * Will fail if another transaction is already inner lock.
	 * <p>
	 * May fail or block on obtaining the outer lock.
	 * <p>
	 * Accomplishes one of the following
	 * <ul>
	 * <li>Blocks on waiting for the outer lock.
	 * <li>takes the outer application lock</li>
	 * <li>throws a {@link SQLRecoverableException} with a message that a
	 * transaction is already in progress</li>
	 * </ul>
	 */
	private void getKeyedOuterLock(Connection outerConnection, String lockName)
			throws SQLException, SQLRecoverableException {

		if (lockName == null) {
			throw new SQLException("Null lockName for outer lock.");
		}

		String innerLockName = lockName;
		String outerLockName = lockName + "_OUT";

		/*
		 * double checked locking optimization on both locks
		 */
		validateLockAvailable(outerConnection, innerLockName);
		validateLockAvailable(outerConnection, outerLockName);

		getAppLock(outerConnection, outerLockName);

	}

	/**
	 * Should only be called inside a transaction.
	 * <p>
	 * Accomplishes one of the following
	 * <ul>
	 * <li>takes an application lock for the subscriber using the subscriber's
	 * id as its resource name.</li>
	 * <li>throws a {@link SQLRecoverableException} with a message that a
	 * transaction is already in progress</li>
	 * <li>throws a {@link SQLException} because the subscriber requested does
	 * not exist or cannot be accessed</li>
	 * </ul>
	 */
	private void getKeyedInnerLock(Connection innerConnection,
			String innerLockName) throws SQLException, SQLRecoverableException {

		if (innerLockName == null) {
			throw new SQLException("Null lockName for inner lock.");
		}

		/*
		 * double check locking optimization on inner lock
		 */
		validateLockAvailable(innerConnection, innerLockName);
		getAppLock(innerConnection, innerLockName);

	}

	private String str(String val) throws SQLException {
		if (val == null || val.equals("")) {
			throw new SQLException("this value " + val
					+ " cannot be empty in db");
		}

		return "'" + apostrophe(val) + "'";
	}

	private String apostrophe(String val) {
		if (val != null) {
			val = val.replaceAll("'", "''");
		}
		return val;
	}

	private void throwRecoverableException(String lockName)
			throws SQLRecoverableException {
		throw new SQLRecoverableException(
				"A transaction is already in progress for lock: " + lockName);
	}

	private void validateLockAvailable(Connection con, String lockName)
			throws SQLRecoverableException, SQLException {

		StringBuilder builder = new StringBuilder();
		builder.append(" SELECT APPLOCK_TEST ( 'public', ");
		builder.append(str(lockName));
		builder.append(" , 'Exclusive' , 'Transaction' ) ");
		builder.append(" as LOCK_AVAILABLE ");
		String cmd = builder.toString();
		ResultSet rs = Db.executeQuery(con, cmd);
		rs.next();
		boolean isLockAvailalbe = rs.getBoolean("LOCK_AVAILABLE");
		if (!isLockAvailalbe) {
			throwRecoverableException(lockName);
		}

	}

	/**
	 * 
	 * @param con
	 * @param lockName
	 * @throws SQLException
	 */
	private void getAppLock(Connection con, String lockName)
			throws SQLException, SQLRecoverableException {

		try {
			StringBuilder builder = new StringBuilder();
			builder.append(" DECLARE @aplsRes INT ");
			builder.append(" EXEC @aplsRes = sp_getapplock ");
			builder.append(" @Resource =  ");
			builder.append(str(lockName));
			builder.append(" ,@LockMode = 'Exclusive' ");
			builder.append(" ,@LockOwner = 'Transaction' ");
			builder.append(" ,@LockTimeout = '60000' "); // 1 minute timeout

			// 0 means success, 1 means success after wait
			builder.append(" IF @aplsRes NOT IN (0, 1) ");
			builder.append(" BEGIN ");
			builder.append("     RAISERROR ( 'TRANSACTION_IN_PROGRESS', 16, 1 ) ");
			builder.append(" END ");
			String cmd = builder.toString();

			Db.execute(con, cmd);
		} catch (SQLException ex) {

			if ("TRANSACTION_IN_PROGRESS".equals(ex.getMessage())) {
				throwRecoverableException(lockName);
			} else {
				throw ex;
			}
		}

	}

}