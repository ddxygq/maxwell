package com.zendesk.maxwell;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MaxwellMysqlStatus {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellMysqlStatus.class);
	private Connection connection;

	public MaxwellMysqlStatus(Connection c) {
		this.connection = c;
	}

	/**
	 * 获取变量值
	 * @param variableName
	 * @return
	 */
	private String sqlStatement(String variableName) {
		return "SHOW VARIABLES LIKE '" + variableName + "'";
	}

	/**
	 * 获取变量值
	 * @param variableName
	 * @param throwOnMissing
	 * @return
	 * @throws SQLException
	 * @throws MaxwellCompatibilityError
	 */
	private String getVariableState(String variableName, boolean throwOnMissing) throws SQLException, MaxwellCompatibilityError {
		ResultSet rs;

		rs = connection.createStatement().executeQuery(sqlStatement(variableName));
		String status;
		if(!rs.next()) {
			if ( throwOnMissing ) {
				throw new MaxwellCompatibilityError("Could not check state for Mysql variable: " + variableName);
			} else {
				return null;
			}
		}

		status = rs.getString("Value");
		return status;
	}

	/**
	 * 校验变量值 如果不符合则报错
	 * @param variable
	 * @param state
	 * @throws SQLException
	 * @throws MaxwellCompatibilityError
	 */
	private void ensureVariableState(String variable, String state) throws SQLException, MaxwellCompatibilityError
	{
		if (!getVariableState(variable, true).equals(state)) {
			throw new MaxwellCompatibilityError("variable " + variable + " must be set to '" + state + "'");
		}
	}

	/**
	 * 校验是否设置 ServerID
	 * @throws SQLException
	 * @throws MaxwellCompatibilityError
	 */
	private void ensureServerIDIsSet() throws SQLException, MaxwellCompatibilityError {
		String id = getVariableState("server_id", false);
		if ( "0".equals(id) ) {
			throw new MaxwellCompatibilityError("server_id is '0'.  Maxwell will not function without a server_id being set.");
		}
	}

	private void ensureRowImageFormat() throws SQLException, MaxwellCompatibilityError {
		String rowImageFormat = getVariableState("binlog_row_image", false);
		if ( rowImageFormat == null ) // only present in mysql 5.6+
			return;

		if ( rowImageFormat.equals("MINIMAL") ) {
			LOGGER.warn("Warning: binlog_row_image is set to MINIMAL.  This may not be what you want.");
			LOGGER.warn("See http://maxwells-daemon.io/compat for more information.");
		}
	}

	/**
	 * 校验复制库的信息
	 * @param c
	 * @throws SQLException
	 * @throws MaxwellCompatibilityError
	 */
	public static void ensureReplicationMysqlState(Connection c) throws SQLException, MaxwellCompatibilityError {
		MaxwellMysqlStatus m = new MaxwellMysqlStatus(c);

		m.ensureServerIDIsSet();
		m.ensureVariableState("log_bin", "ON");
		m.ensureVariableState("binlog_format", "ROW");
		m.ensureRowImageFormat();
	}
	/**
	 * 校验数据库是否设置只读，如果设置为只读则报错
	 * @param c
	 * @throws SQLException
	 * @throws MaxwellCompatibilityError
	 */
	public static void ensureMaxwellMysqlState(Connection c) throws SQLException, MaxwellCompatibilityError {
		MaxwellMysqlStatus m = new MaxwellMysqlStatus(c);

		m.ensureVariableState("read_only", "OFF");
	}

	/**
	 * 校验数据库gtid模式参数是否符合要求，如果不符合则报错
	 * @param c
	 * @throws SQLException
	 * @throws MaxwellCompatibilityError
	 */
	public static void ensureGtidMysqlState(Connection c) throws SQLException, MaxwellCompatibilityError {
		MaxwellMysqlStatus m = new MaxwellMysqlStatus(c);

		m.ensureVariableState("gtid_mode", "ON");
		m.ensureVariableState("log_slave_updates", "ON");
		m.ensureVariableState("enforce_gtid_consistency", "ON");
	}
}
