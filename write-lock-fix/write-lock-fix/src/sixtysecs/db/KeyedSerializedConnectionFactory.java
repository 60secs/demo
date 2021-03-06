package sixtysecs.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;

import org.apache.log4j.Logger;

/**
 * Provides blocking connections which are guaranteed to have a unique
 * identifier. This overcomes write skew anomaly limitations of Sql Server's
 * read committed, snapshot isolation using multi-versioned concurrency control.
 * <p>
 * Uses a double mutex to ensure that if blocking does occur on the first
 * (outer) mutex, the transactional connection returned will reflect the state
 * of the database after the outer lock was obtained instead of the state when
 * the lock was requested.
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
	 * May block or throw if another transaction already has the same keyed lock
	 * <p>
	 * Accomplishes one of the following
	 * <ul>
	 * <li>Blocks on waiting for the keyed lock.
	 * <li>takes the keyed lock lock</li>
	 * <li>throws a {@link SQLRecoverableException} with a message that a
	 * transaction is already in progress</li>
	 * </ul>
	 * 
	 * Should only and always be called if all of the following conditions are
	 * true:
	 * <ul>
	 * <li>It is allowed for the operation about to be performed to be retried
	 * on failure.</li>
	 * <li>A unique identifier is known for the data being modified</li>
	 * </ul>
	 */
	public Connection getKeyedSerializedConnection(String lockName)
			throws SQLException, SQLRecoverableException {
		Connection outerConnection = null;

		if (lockName == null || lockName.length() == 0
				|| lockName.length() > 250) {
			throw new IllegalArgumentException(
					"lockName length must be between 1 and 250 characters");
		}

		final String innerLockName = lockName;
		final String outerLockName = lockName + "_OUT";


		try {
			outerConnection = dbConnectionFactory.getNewTransactionConnection();
			performDmlToBeginTransactionContext(outerConnection);

			/*
			 * double checked locking optimization on both locks
			 */
			validateLockAvailable(outerConnection, innerLockName);
			validateLockAvailable(outerConnection, outerLockName);

			getAppLock(outerConnection, outerLockName);
			
		} catch (SQLException ex) {
			if (outerConnection != null) {
				outerConnection.rollback();
			}
			throw ex;
		}

		Connection innerConnection = null; // TODO: refactor to inner
											// connection
		try {
			innerConnection = dbConnectionFactory.getNewTransactionConnection();
			performDmlToBeginTransactionContext(innerConnection);

			/*
			 * double check locking optimization on inner lock
			 */
			validateLockAvailable(innerConnection, innerLockName);
			getAppLock(innerConnection, innerLockName);
			
		} catch (SQLException ex) {
			if (innerConnection != null) {
				innerConnection.rollback();
			}
			throw ex;
		} finally {
			/**
			 * Ok to release outer lock because attempts to grab outer lock will
			 * fail if inner lock is still being held.
			 * <p>
			 * It is important to rollback the outer connection now so that only
			 * 1 connection will use db resources and require management by the
			 * user.
			 */
			outerConnection.rollback();
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
		{

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

	private void throwRecoverableException(String lockName)
			throws SQLRecoverableException {
		throw new SQLRecoverableException(
				"A transaction is already in progress for lock: " + lockName);
	}

	private void validateLockAvailable(Connection con, String lockName)
			throws SQLRecoverableException, SQLException {

		StringBuilder builder = new StringBuilder();
		builder.append(" SELECT APPLOCK_TEST ( 'public', ");
		builder.append(Db.str(lockName));
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

	private void getAppLock(Connection con, String lockName)
			throws SQLException, SQLRecoverableException {

		try {
			StringBuilder builder = new StringBuilder();
			builder.append(" DECLARE @aplsRes INT ");
			builder.append(" EXEC @aplsRes = sp_getapplock ");
			builder.append(" @Resource =  ");
			builder.append(Db.str(lockName));
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