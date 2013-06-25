package sixtysecs.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;

import org.apache.log4j.Logger;

//TODO: throw if over length allowed:
//TODO: use smaller suffix to allow greater range (e.g. 250)
/**
 * TODO: contract include db options MVCC row isolation and read committed
 * 
 * @author Eric Driggs
 * 
 */

// TODO: rename
public class KeyedSerializedConnectionFactory {
	final static boolean autoCommit = false;
	final String existingTableName ;
	protected static Logger logger = Logger.getRootLogger();
	ConnectionFactory dbConnectionFactory;

	public KeyedSerializedConnectionFactory(String connectionString, String existingTableName)
			throws SQLException {
		this.dbConnectionFactory = new ConnectionFactory(connectionString);
		this.existingTableName = existingTableName;
	}

	/**
	 * Returns a new transaction which requires a commit to save changes.
	 * <p>
	 * Only one keyedLockTransaction per key string can be run at a time for a
	 * given key string. Additional attempts while the first transaction for a
	 * key string are running will either throw or block.
	 * <p>
	 * Should only and always be called if all of the following conditions are
	 * true:
	 * <ul>
	 * <li>It is normal for the operation about to be performed to be retried on
	 * failure.</li>
	 * </ul>
	 * <p>
	 * Do not call for subscriber operations. Use
	 * {@link Connection#newSubscriberLockTransactWriteSkewFix)} instead for
	 * subscriber operations.
	 * 
	 * @param key
	 *            the string key to lock on.
	 * @return a transaction which has an application lock with resource name
	 *         set to the key string
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

		// FIXME: change subscriber comment
		/*
		 * The outer lock can only be taken if both the inner and outer lock are
		 * available. If a subscriber transaction is in progress, an attempt to
		 * get a duplicate subscriber transaction may either fail or block. <p>
		 * By using a separate transaction for the outer lock, we ensure that if
		 * blocking does occur on the outer lock, the transaction returned will
		 * reflect the state of the database after the outer lock was obtained
		 * instead of the state when the lock was requested.
		 */
		try {
			outerLockTransaction = dbConnectionFactory
					.getNewTransactionConnection();
			primeConnectionForAppLock(outerLockTransaction);

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

		Connection innerLockTransaction = null; // TODO: refactor to inner
												// connection
		try {
			innerLockTransaction = dbConnectionFactory
					.getNewTransactionConnection();
			/* ObtainWriteSkewFixk in fresh transaction context we will return */
			primeConnectionForAppLock(innerLockTransaction);
			getKeyedInnerLock(innerLockTransaction, lockName);
		} catch (SQLException ex) {
			if (innerLockTransaction != null) {
				innerLockTransaction.rollback();
			}
			throw ex;
		} finally {
			/*
			 * Ok to release outer lock because attempts to grab outer lock will
			 * fail if inner lock is still being held
			 */
			outerLockTransaction.rollback();
		}
		return outerLockTransaction;
	}

	/**
	 * SQL Server does not respect a database connection's app lock unless a
	 * select is performed against an existing table in the same database first.
	 * 
	 * @throws SQLException
	 * @throws SQLRecoverableException
	 */
	private void primeConnectionForAppLock(Connection con) throws SQLException,
			SQLRecoverableException {
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
	 * <li>takes an application lock for the subscriber using the subscriber's
	 * id + "_OUTER" as its resource name.</li>
	 * <li>throws a {@link SQLRecoverableException} with a message that a
	 * transaction is already in progress</li>
	 * <li>throws a {@link SQLException} because the subscriber requested does
	 * not exist or cannot be accessed</li>
	 * </ul>
	 */
	private void getKeyedOuterLock(Connection outerConnection, String lockName)
			throws SQLException, SQLRecoverableException {

		if (lockName == null) {
			throw new SQLException("Null lockName for outer lock.");
		}

		String innerLockName = lockName;
		String outerLockName = lockName + "_OUTER";

		// See if the inner lock is available but do not try to take it
		// yet
		// TOMAYBE: is this a necessary check?
		try {
			StringBuilder builder = new StringBuilder();
			builder.append(" SELECT APPLOCK_TEST ( 'public', ");
			builder.append(str(innerLockName));
			builder.append(" , 'Exclusive' , 'Transaction' ) ");
			builder.append(" as LOCK_AVAILABLE ");
			String cmd = builder.toString();
			logger.debug("appLockSubscriber: " + cmd);
			ResultSet rs = Db.executeQuery(outerConnection, cmd);
			rs.next();
			boolean isLockAvailalbe = rs.getBoolean("LOCK_AVAILABLE");
			if (!isLockAvailalbe) {
				throwRecoverableException(lockName);
			}

		} catch (SQLException ex) {
			if ("TRANSACTION_IN_PROGRESS".equals(ex.getMessage())) {
				throwRecoverableException(lockName);
			} else {
				throw ex;
			}
		}

		// See if the outer lock is available but do not try to take it
		// yet
		try {
			StringBuilder builder = new StringBuilder();
			builder.append(" SELECT APPLOCK_TEST ( 'public', ");
			builder.append(str(outerLockName));
			builder.append(" , 'Exclusive' , 'Transaction' ) ");
			builder.append(" as LOCK_AVAILABLE ");
			String cmd = builder.toString();
			logger.debug("appLockSubscriber: " + cmd);
			ResultSet rs = Db.executeQuery(outerConnection, cmd);
			rs.next();
			boolean isLockAvailalbe = rs.getBoolean("LOCK_AVAILABLE");
			if (!isLockAvailalbe) {
				throwRecoverableException(outerLockName);
			}
		} catch (SQLException ex) {
			if ("TRANSACTION_IN_PROGRESS".equals(ex.getMessage())) {
				throwRecoverableException(outerLockName);
			} else {
				throw ex;
			}
		}

		// try to take the application lock
		try {
			StringBuilder builder = new StringBuilder();
			builder.append(" DECLARE @aplsRes2 INT ");
			builder.append(" EXEC @aplsRes2 = sp_getapplock ");
			builder.append(" @Resource =  ");
			builder.append(str(outerLockName));
			builder.append(" ,@LockMode = 'Exclusive' ");
			builder.append(" ,@LockOwner = 'Transaction' ");
			// 1 minute timeout
			builder.append(" ,@LockTimeout = '60000' ");
			// FIXME: if the result is 1, we should recreate the
			// transaction after this returns
			builder.append(" IF @aplsRes2 NOT IN (0, 1) ");
			builder.append(" BEGIN ");
			builder.append("     RAISERROR ( 'TRANSACTION_IN_PROGRESS', 16, 1 ) ");
			builder.append(" END ");
			String cmd = builder.toString();
			logger.debug("appLockSubscriber2: " + cmd);
			Db.execute(outerConnection, cmd);
		} catch (SQLException ex) {
			// TODO: get the message make sure not null
			if ("TRANSACTION_IN_PROGRESS".equals(ex.getMessage())) {
				throwRecoverableException(outerLockName);
			} else {
				throw ex;
			}
		}

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
	private void getKeyedInnerLock(Connection innerConnection, String lockName)
			throws SQLException, SQLRecoverableException {

		if (lockName == null) {
			throw new SQLException("Null lockName for inner lock.");
		}

		// See if the lock is available but do not try to take it yet

		try {
			StringBuilder builder = new StringBuilder();
			builder.append(" SELECT APPLOCK_TEST ( 'public', ");
			builder.append(str(lockName));
			builder.append(" , 'Exclusive' , 'Transaction' ) ");
			builder.append(" as LOCK_AVAILABLE ");
			String cmd = builder.toString();
			logger.debug("appLockSubscriber: " + cmd);
			ResultSet rs = Db.executeQuery(innerConnection, cmd);
			rs.next();
			boolean isLockAvailalbe = rs.getBoolean("LOCK_AVAILABLE");
			if (!isLockAvailalbe) {
				throwRecoverableException(lockName);
			}

		} catch (SQLException ex) {
			if ("TRANSACTION_IN_PROGRESS".equals(ex.getMessage())) {
				throwRecoverableException(lockName);
			} else {
				throw ex;
			}
		}

		// try to take the application lock
		try {
			StringBuilder builder = new StringBuilder();
			builder.append(" DECLARE @aplsRes2 INT ");
			builder.append(" EXEC @aplsRes2 = sp_getapplock ");
			builder.append(" @Resource =  ");
			builder.append(str(lockName));
			builder.append(" ,@LockMode = 'Exclusive' ");
			builder.append(" ,@LockOwner = 'Transaction' ");
			// 1 minute timeout
			builder.append(" ,@LockTimeout = '60000' ");
			// FIXME: if the result is 1, we should recreate the
			// transaction after this returns
			builder.append(" IF @aplsRes2 NOT IN (0, 1) ");
			builder.append(" BEGIN ");
			builder.append("     RAISERROR ( 'TRANSACTION_IN_PROGRESS', 16, 1 ) ");
			builder.append(" END ");
			String cmd = builder.toString();
			logger.debug("appLockSubscriber2: " + cmd);
			Db.execute(innerConnection, cmd);
		} catch (SQLException ex) {
			// TODO: get the message make sure not null
			if ("TRANSACTION_IN_PROGRESS".equals(ex.getMessage())) {
				throwRecoverableException(lockName);
			} else {
				throw ex;
			}
		}

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

}