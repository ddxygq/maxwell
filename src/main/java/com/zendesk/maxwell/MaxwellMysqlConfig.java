package com.zendesk.maxwell;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.replication.BinlogConnectorReplicator;
import org.apache.commons.lang3.StringUtils;

import joptsimple.OptionSet;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kaufmannkr on 12/23/15.
 */
public class MaxwellMysqlConfig {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellMysqlConfig.class);
	public String host;
	public Integer port;
	public String database;
	public String user;
	public String password;
	public Integer replicationPort;
	public String  replicationUser;
	public String  replicationPassword;
	public String  replicationHost;
	public SSLMode sslMode;
	public Map<String, String> jdbcOptions = new HashMap<>();
	public Integer connectTimeoutMS = 5000;

	public MaxwellMysqlConfig() {
		this.host = null;
		this.port = null;
		this.database = null;
		this.user = null;
		this.password = null;
		this.replicationPort = null;
		this.replicationUser = null;
		this.replicationPassword = null;
		this.replicationHost = null;
		this.sslMode = null;

		this.jdbcOptions = new HashMap<>();
		this.jdbcOptions.put("zeroDateTimeBehavior", "convertToNull");
		this.jdbcOptions.put("connectTimeout", String.valueOf(connectTimeoutMS));
	}

	public MaxwellMysqlConfig(String host, Integer port, String database, String user, String password,
			SSLMode sslMode) {
		this.host = host;
		this.port = port;
		this.database = database;
		this.user = user;
		this.password = password;
		this.sslMode = sslMode;
	}
	public MaxwellMysqlConfig(String host, Integer port, String database, String user, String password,String  replicationHost,
							  Integer replicationPort,String  replicationUser,String  replicationPassword,SSLMode sslMode) {
		this.host = host;
		this.port = port;
		this.database = database;
		this.user = user;
		this.password = password;
		this.replicationPort = replicationPort;
		this.replicationUser = replicationUser;
		this.replicationPassword = replicationPassword;
		this.replicationHost = replicationHost;
		this.sslMode = sslMode;
	}

	private void useSSL(boolean should) {
		this.jdbcOptions.put("useSSL", String.valueOf(should));
	}

	private void requireSSL(boolean should) {
		this.jdbcOptions.put("requireSSL", String.valueOf(should));
	}

	private void verifyServerCertificate(boolean should) {
		this.jdbcOptions.put("verifyServerCertificate", String.valueOf(should));
	}

	public void setJDBCOptions(String opts) {
		if (opts == null)
			return;

		for ( String opt : opts.split("&") ) {
			String[] valueKeySplit = opt.trim().split("=", 2);
			if (valueKeySplit.length == 2) {
				this.jdbcOptions.put(valueKeySplit[0], valueKeySplit[1]);
			}
		}
	}

	private void setSSLOptions() {
		if (sslMode != null && sslMode != SSLMode.DISABLED) {
			this.useSSL(true); // for all SSL modes other than DISABLED, use SSL

			this.verifyServerCertificate(false); // default to not verify server cert
			this.requireSSL(false); // default to not require SSL

			this.requireSSL(sslMode == SSLMode.REQUIRED || sslMode == SSLMode.VERIFY_CA
					|| sslMode == SSLMode.VERIFY_IDENTITY);

			this.verifyServerCertificate(sslMode == SSLMode.VERIFY_IDENTITY);
		}
		else {
			this.useSSL(false);
		}
	}

	public String getConnectionURI(boolean includeDatabase) throws URISyntaxException {
		return getConnection(includeDatabase,host,port);
	}

	private String getConnection(boolean includeDatabase,String host,Integer port) throws URISyntaxException {
		this.setSSLOptions();
		URIBuilder uriBuilder = new URIBuilder();
		uriBuilder.setScheme("jdbc:mysql");
		uriBuilder.setHost(host);
		uriBuilder.setPort(port);
		if (database != null && includeDatabase) {
			uriBuilder.setPath("/" + database);
		}
		for (Map.Entry<String, String> jdbcOption : jdbcOptions.entrySet()) {
			uriBuilder.addParameter(jdbcOption.getKey(), jdbcOption.getValue());
		}
		// added by d8888 2018/09/10, force JDBC to use UTF-8 to support using non-english db, table & column names
		uriBuilder.addParameter("characterEncoding", "UTF-8");
		uriBuilder.addParameter("tinyInt1isBit", "false");
		LOGGER.info("getConnection-->host:{},uriBuilder.build():{}",host,uriBuilder.build().toString());
		return uriBuilder.build().toString();
	}

	public String getReplicationConnectionURI(boolean includeDatabase) throws URISyntaxException {
		return getConnection(includeDatabase,replicationHost,replicationPort);
//		this.setSSLOptions();
//		URIBuilder uriBuilder = new URIBuilder();
//		uriBuilder.setScheme("jdbc:mysql");
//		uriBuilder.setHost(replicationHost);
//		uriBuilder.setPort(replicationPort);
//		if (database != null && includeDatabase) {
//			uriBuilder.setPath("/" + database);
//		}
//		for (Map.Entry<String, String> jdbcOption : jdbcOptions.entrySet()) {
//			uriBuilder.addParameter(jdbcOption.getKey(), jdbcOption.getValue());
//		}
//		uriBuilder.addParameter("characterEncoding", "UTF-8");
//		uriBuilder.addParameter("tinyInt1isBit", "false");
//		return uriBuilder.build().toString();
	}

	public String getConnectionURI() throws URISyntaxException { return getConnectionURI(true); }
	public String getReplicationConnectionURI() throws URISyntaxException { return getReplicationConnectionURI(true); }

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		MaxwellMysqlConfig that = (MaxwellMysqlConfig) o;
		return Objects.equals(host, that.host) &&
				Objects.equals(port, that.port) &&
				Objects.equals(database, that.database) &&
				Objects.equals(user, that.user) &&
				Objects.equals(password, that.password) &&
				sslMode == that.sslMode &&
				Objects.equals(jdbcOptions, that.jdbcOptions) &&
				Objects.equals(connectTimeoutMS, that.connectTimeoutMS);
	}

	public boolean sameServerAs(MaxwellMysqlConfig other) {
		return Objects.equals(host, other.host) &&
			Objects.equals(port, other.port);
	}

	@Override
	public int hashCode() {
		return Objects
				.hash(host, port, database, user, password, sslMode, jdbcOptions, connectTimeoutMS);
	}
}
