package com.zendesk.maxwell.bootstrap;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.replication.Replicator;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.TimeColumnDef;
import com.zendesk.maxwell.scripting.Scripting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class SynchronousBootstrapper extends AbstractBootstrapper {
	static final Logger LOGGER = LoggerFactory.getLogger(SynchronousBootstrapper.class);
	private static final long INSERTED_ROWS_UPDATE_PERIOD_MILLIS = 250;

	private long lastInsertedRowsUpdateTimeMillis = 0;

	public SynchronousBootstrapper(MaxwellContext context) { super(context); }

	@Override
	public boolean shouldSkip(RowMap row) {
		// the synchronous bootstrapper blocks other incoming messages
		// to the replication stream so there's nothing to skip
		return false;
	}

	@Override
	public void startBootstrap(RowMap startBootstrapRow, AbstractProducer producer, Replicator replicator) throws Exception {
		LOGGER.info("synch-->startBootstrap-->row:{},producer:{},replicator:{}",startBootstrapRow.getData(),producer,null!=replicator.getRow()?replicator.getRow():"null");
		String databaseName = bootstrapDatabase(startBootstrapRow);
		String tableName = bootstrapTable(startBootstrapRow);

		String whereClause = bootstrapWhere(startBootstrapRow);

		String logString = String.format("bootstrapping request for %s.%s", databaseName, tableName);
		if ( whereClause != null ) {
			logString += String.format(" with where clause %s", whereClause);
		}
		LOGGER.debug(logString);

		Schema schema = replicator.getSchema();
		Database database = findDatabase(schema, databaseName);
		Table table = findTable(tableName, database);

		Long schemaId = replicator.getSchemaId();
		Position position = startBootstrapRow.getPosition();
		LOGGER.info("startBootstrap-->insert position:{}",position);
//		producer.push(startBootstrapRow);
//		producer.push(bootstrapStartRowMap(table, position));
		LOGGER.info(String.format("bootstrapping started for %s.%s, binlog position is %s", databaseName, tableName, position.toString()));
		try ( Connection connection = getConnection();
			  Connection streamingConnection = getStreamingConnection(databaseName)) {
			/**
			 * 开启bootstrap任务，修改bootstrap表中记录
			 */
			setBootstrapRowToStarted(startBootstrapRow, connection);
			/**
			 * 开启bootstrap任务，获取别监控表的记录
			 */
			ResultSet resultSet = getAllRows(databaseName, tableName, schema, whereClause, streamingConnection);
			int insertedRows = 0;
			lastInsertedRowsUpdateTimeMillis = 0; // ensure updateInsertedRowsColumn is called at least once
			while ( resultSet.next() ) {
				RowMap row = bootstrapEventRowMap("bootstrap-insert", table, position);
				setRowValues(row, resultSet, table);
				row.setSchemaId(schemaId);

				Scripting scripting = context.getConfig().scripting;
				if ( scripting != null )
					scripting.invoke(row);

				if ( LOGGER.isDebugEnabled() )
					LOGGER.debug("bootstrapping row : " + row.toJSON());

				producer.push(row);
				++insertedRows;
				updateInsertedRowsColumn(insertedRows, startBootstrapRow, position.getBinlogPosition(), connection);
			}
			setBootstrapRowToCompleted(insertedRows, startBootstrapRow, connection);
		}
	}

	/**
	 * 更新插入完成记录数
	 * @param insertedRows 完成数
	 * @param startBootstrapRow
	 * @param position
	 * @param connection
	 * @throws SQLException
	 * @throws NoSuchElementException
	 */
	private void updateInsertedRowsColumn(int insertedRows, RowMap startBootstrapRow, BinlogPosition position, Connection connection) throws SQLException, NoSuchElementException {
		long now = System.currentTimeMillis();
		if ( now - lastInsertedRowsUpdateTimeMillis > INSERTED_ROWS_UPDATE_PERIOD_MILLIS ) {
			long rowId = ( long ) startBootstrapRow.getData("id");
			String sql = "update `bootstrap` set inserted_rows = ?, binlog_file = ?, binlog_position = ? where id = ?";
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setInt(1, insertedRows);
			preparedStatement.setString(2, position.getFile());
			preparedStatement.setLong(3, position.getOffset());
			preparedStatement.setLong(4, rowId);
			if ( preparedStatement.executeUpdate() == 0 ) {
				throw new NoSuchElementException();
			}
			lastInsertedRowsUpdateTimeMillis = now;
		}
	}
	/**
	 * 监控maxwell存储表的连接信息
	 * @return
	 * @throws SQLException
	 * @throws URISyntaxException
	 */
	protected Connection getConnection() throws SQLException {
		//12-16 替换连接为Maxwell
//		Connection conn = context.getReplicationConnection();
		Connection conn = context.getMaxwellConnection();
		conn.setCatalog(context.getConfig().databaseName);
		LOGGER.info("synch-->getConnection-->conn:{}",conn.getClass().getResource("url"));
		return conn;
	}

	/**
	 * 被监控数据库的连接信息
	 * @return
	 * @throws SQLException
	 * @throws URISyntaxException
	 */
	protected Connection getStreamingConnection(String databaseName) throws SQLException, URISyntaxException {
		Connection conn = DriverManager.getConnection(context.getConfig().replicationMysql.getReplicationConnectionURI(), context.getConfig().replicationMysql.replicationUser, context.getConfig().replicationMysql.replicationPassword);
		//12-16 替换连接为被监控的数据库名
//		conn.setCatalog(context.getConfig().databaseName);
		conn.setCatalog(databaseName);
//		LOGGER.info("synch-->getStreamingConnection-->conn:{}",conn.getClass().getResource("url"));
		return conn;
	}

	private RowMap bootstrapStartRowMap(Table table, Position position) {
		return bootstrapEventRowMap("bootstrap-start", table, position);
	}

	private RowMap bootstrapCompleteRowMap(Table table, Position position) {
		return bootstrapEventRowMap("bootstrap-complete", table, position);
	}

	private RowMap bootstrapEventRowMap(String type, Table table, Position position) {
		return new RowMap(
				type,
				table.getDatabase(),
				table.getName(),
				System.currentTimeMillis(),
				table.getPKList(),
				position);
	}

	@Override
	public void completeBootstrap(RowMap completeBootstrapRow, AbstractProducer producer, Replicator replicator) throws Exception {
		String databaseName = bootstrapDatabase(completeBootstrapRow);
		String tableName = bootstrapTable(completeBootstrapRow);

		Database database = findDatabase(replicator.getSchema(), databaseName);
		ensureTable(tableName, database);
		Table table = findTable(tableName, database);

		Position position = completeBootstrapRow.getPosition();
//		producer.push(completeBootstrapRow);
//		producer.push(bootstrapCompleteRowMap(table, position));

		LOGGER.info(String.format("bootstrapping ended for %s.%s", databaseName, tableName));
	}

	@Override
	public void resume(AbstractProducer producer, Replicator replicator) throws Exception {
		try ( Connection connection = context.getMaxwellConnection() ) {
			// This update resets all rows of incomplete bootstraps to their original state.
			// These updates are treated as fresh bootstrap requests and trigger a restart
			// of the bootstrap process from the beginning.
			String clientID = this.context.getConfig().clientID;
			String sql = "update `bootstrap` set started_at = NULL where is_complete = 0 and started_at is not NULL and client_id = ?";
			PreparedStatement s = connection.prepareStatement(sql);
			s.setString(1, clientID);
			s.execute();
		}
	}

	@Override
	public boolean isRunning( ) {
		return false;
	}

	@Override
	public void work(RowMap row, AbstractProducer producer, Replicator replicator) throws Exception {
		try {
			if ( isStartBootstrapRow(row) ) {
//				LOGGER.info("synch-->startBootstrap-->row:{},producer:{},replicator:{}",row.getData(),producer,null!=replicator.getRow()?replicator.getRow():"null");
				startBootstrap(row, producer, replicator);
			} else if ( isCompleteBootstrapRow(row) ) {
//				LOGGER.info("synch-->completeBootstrap-->row:{},producer:{},replicator:{}",row.getData(),producer,null!=replicator.getRow()?replicator.getRow():"null");
				completeBootstrap(row, producer, replicator);
			}
		} catch ( NoSuchElementException e ) {
			LOGGER.info(String.format("bootstrapping cancelled for %s.%s", row.getDatabase(), row.getTable()));
		}
	}

	@Override
	public void workForBootstrape(RowMap row, AbstractProducer producer, Replicator replicator) throws Exception {

	}

	@Override
	public void startBootstrapForSeparate(RowMap startBootstrapRow, AbstractProducer producer, Replicator replicator) throws Exception {

	}

	@Override
	public void completeBootstrapForSeparate(RowMap completeBootstrapRow, AbstractProducer producer, Replicator replicator) throws Exception {

	}

	private Table findTable(String tableName, Database database) {
		Table table = database.findTable(tableName);
		if ( table == null )
			throw new RuntimeException("Couldn't find table " + tableName);
		return table;
	}

	private Database findDatabase(Schema schema, String databaseName) {
		Database database = schema.findDatabase(databaseName);
		if ( database == null )
			throw new RuntimeException("Couldn't find database " + databaseName);
		return database;
	}

	private void ensureTable(String tableName, Database database) {
		findTable(tableName, database);
	}

	/**
	 * 查询所有数据
	 * @param databaseName
	 * @param tableName
	 * @param schema
	 * @param whereClause
	 * @param connection
	 * @return
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	private ResultSet getAllRows(String databaseName, String tableName, Schema schema, String whereClause,
								Connection connection) throws SQLException, InterruptedException {
		Statement statement = createBatchStatement(connection);
		String pk = schema.findDatabase(databaseName).findTable(tableName).getPKString();

		String sql = String.format("select * from `%s`.%s", databaseName, tableName);

		if ( whereClause != null && !whereClause.equals("") ) {
			sql += String.format(" where %s", whereClause);
		}

		if ( pk != null && !pk.equals("") ) {
			sql += String.format(" order by %s", pk);
		}
		LOGGER.info("bootstrapping getAllRows sql:{}", sql);
		return statement.executeQuery(sql);
	}

	/**
	 * 设置批量处理日志信息，当statement设置以下属性时，采用的是流数据接收方式，每次只从服务器接收部份数据，直到所有数据处理完毕，不会发生JVM OOM
	 * @param connection
	 * @return
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	private Statement createBatchStatement(Connection connection) throws SQLException, InterruptedException {
		Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		statement.setFetchSize(Integer.MIN_VALUE);
		return statement;
	}

	/**
	 * 开启bootstrap任务
	 * @param startBootstrapRow bootstrap任务
	 * @param connection
	 * @throws SQLException
	 * @throws NoSuchElementException
	 */
	private void setBootstrapRowToStarted(RowMap startBootstrapRow, Connection connection) throws SQLException, NoSuchElementException {
		String sql = "update `bootstrap` set started_at=NOW() where id=?";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setLong(1, ( Long ) startBootstrapRow.getData("id"));
		if ( preparedStatement.executeUpdate() == 0) {
			throw new NoSuchElementException();
		}
	}

	/**
	 * 设置bootstrap任务完成
	 * @param insertedRows 插入数据
	 * @param startBootstrapRow
	 * @param connection 监控maxwell存储表的连接信息
	 * @throws SQLException
	 * @throws NoSuchElementException
	 */
	private void setBootstrapRowToCompleted(int insertedRows, RowMap startBootstrapRow, Connection connection) throws SQLException, NoSuchElementException {
		LOGGER.info("setBootstrapRowToCompleted-->row:{},insertedRows:{}",startBootstrapRow.getData(),insertedRows);
		String sql = "update `bootstrap` set is_complete=1, inserted_rows=?, completed_at=NOW() where id=?";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setInt(1, insertedRows);
		preparedStatement.setLong(2, ( Long ) startBootstrapRow.getData("id"));
		if ( preparedStatement.executeUpdate() == 0) {
			LOGGER.error("setBootstrapRowToCompleted-->executeUpdate:更新失败");
			throw new NoSuchElementException();
		}
	}

	private void setRowValues(RowMap row, ResultSet resultSet, Table table) throws SQLException, IOException {
		Iterator<ColumnDef> columnDefinitions = table.getColumnList().iterator();
//		LOGGER.info("setRowValues-->row:{},columnDefinitions:{}",row.toString(),table.getColumnList());
		int columnIndex = 1;
		while ( columnDefinitions.hasNext() ) {
			ColumnDef columnDefinition = columnDefinitions.next();
			Object columnValue;

			// need to explicitly coerce TIME into TIMESTAMP in order to preserve nanoseconds
			if ( columnDefinition instanceof TimeColumnDef )
				columnValue = resultSet.getTimestamp(columnIndex);
			else
				columnValue = resultSet.getObject(columnIndex);

			row.putData(
				columnDefinition.getName(),
				columnValue == null ? null : columnDefinition.asJSON(columnValue)
			);

			++columnIndex;
		}
//		LOGGER.info("setRowValues-->end row:{}",row.toString());
	}

}
