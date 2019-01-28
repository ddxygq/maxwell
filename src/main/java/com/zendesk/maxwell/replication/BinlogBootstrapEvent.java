package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.github.shyiko.mysql.binlog.event.deserialization.MissingTableMapEventException;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * @Auther: lg
 * @Date: 2018/12/14 11:34
 * @Description:this class for host and replicate_host are separate
 */
public class BinlogBootstrapEvent {
	private static final Logger LOGGER = LoggerFactory.getLogger(BinlogBootstrapEvent.class);
	private  MaxwellContext maxwellContext;
	private  BinlogConnectorReplicator replicator;
	private BinaryLogClient client;
	private BinlogConnectorEventListener binlogEventListener;
	private Position initialPosition;
	private Long serverId;
	private Connection connectionReplicate ;
	private Connection connectionMaxwell;
	String databaseName;

	public BinlogBootstrapEvent() {
		this.maxwellContext = null;
		this.replicator = null;
	}

	public BinlogBootstrapEvent(MaxwellContext maxwellContext, BinlogConnectorReplicator replicator) {
		this.maxwellContext = maxwellContext;
		this.replicator = replicator;
		this.client = replicator.getClient();
		this.binlogEventListener = replicator.getBinlogEventListener();
		this.databaseName = maxwellContext.getConfig().databaseName;
		try {
			this.connectionReplicate = maxwellContext.getReplicationConnection();
			this.connectionMaxwell = maxwellContext.getMaxwellConnection();
		} catch (SQLException e) {
			LOGGER.error("bootstrap init connectionReplicate error msg:{}",e.getMessage());
			e.printStackTrace();
		}
	}

	public void setInitialPosition(Position initialPosition) {
		this.initialPosition = initialPosition;
	}
	public MaxwellContext getMaxwellContext() {
		return maxwellContext;
	}

	public void setMaxwellContext(MaxwellContext maxwellContext) {
		this.maxwellContext = maxwellContext;
	}

	public BinlogConnectorReplicator getReplicator() {
		return replicator;
	}

	public void setReplicator(BinlogConnectorReplicator replicator) {
		this.replicator = replicator;
	}

	/**
	 * 处理监控数据库不在同一服务器的情况
	 */
	public void bootstrapEvent() throws IOException, SQLException {
		SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		long time = date.getTime();
		String dateStr = sd.format(date);
		String databaseName = this.databaseName;
		String tableName = "bootstrap";
		String client_id = maxwellContext.getConfig().clientID;
		long serverId = this.maxwellContext.getServerID();
		Position position = this.initialPosition;
		try {
			Connection conn = maxwellContext.getMaxwellConnection();
			ResultSet resultSet =  getStartResult(conn,databaseName,tableName,client_id);
			List<RowMap> list = listRowStartEvent(time,serverId,resultSet,position);
			if(null != list && list.size() > 0) {
				for (RowMap row : list) {
					LOGGER.info("bootstrap事件insert触发了 row:{}", row.toString());
					updateRow(conn,row,databaseName,tableName,1);
					replicator.processBootstrapeRow(row,replicator);
				}
				LOGGER.info("bootstrap事件insert over ...position:{}",position);
				//处理任务结束后update阶段
				ResultSet resultEndSet =  getEndResult(conn,databaseName,tableName,client_id,dateStr);
				List<RowMap> listEnd = listRowEndEvent(time,serverId,resultEndSet,position);
				if(null != listEnd && listEnd.size() > 0) {
					for (RowMap rowMap : listEnd) {
						LOGGER.info("bootstrap事件update触发了 row:{}", rowMap.toString());
						replicator.processBootstrapeRow(rowMap,replicator);
						updateRow(conn,rowMap,databaseName,tableName,2);
					}
					LOGGER.info("bootstrap事件update over ...position:{}",position);
				}
				conn.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public List<RowMap> listRowStartEvent(long time,long serverId,ResultSet resultSet,Position position) throws SQLException {
		List<RowMap> list = new ArrayList<>();
		while ( resultSet.next() ) {
			list.add(getInsertRow(time,serverId,resultSet,position));
		}
		return list;
	}
	public List<RowMap> listRowEndEvent(long time,long serverId,ResultSet resultSet,Position position) throws SQLException {
		List<RowMap> list = new ArrayList<>();
		while ( resultSet.next() ) {
			list.add(getUpdateRow(time,serverId,resultSet,position));
		}
		return list;
	}

	public ResultSet getStartResult(Connection conn,String databaseName,String tableName,String client_id) throws SQLException {
		Statement statement = conn.createStatement();
		String sql = String.format("select id,database_name,table_name,is_complete,started_at,completed_at,client_id,where_clause from `%s`.%s where is_complete=0 and is_trigger =0 and client_id='%s'", databaseName, tableName,client_id);
//		LOGGER.info("自定义事件 查询bootstrap事件 sql:{}", sql);
		ResultSet resultSet = statement.executeQuery(sql);
		return resultSet;
	}
	public ResultSet getEndResult(Connection conn,String databaseName,String tableName,String client_id,String date) throws SQLException {
		Statement statement = conn.createStatement();
		StringBuilder sql = new StringBuilder();
		sql.append("select id,database_name,table_name,is_complete,started_at,completed_at,client_id,where_clause from "+databaseName+"."+tableName);
		sql.append(" where is_complete=1 and is_trigger =1 and date_format(completed_at, '%Y-%m-%d')='"+date+"'");
		sql.append(" and client_id= '"+client_id+"'");
//		String sql = String.format("select id,database_name,table_name,is_complete,started_at,completed_at,client_id,where_clause from `%s`.%s where is_complete=1 and date_format(completed_at, '%tF%n')='%s' and client_id='%s'", databaseName, tableName,new Date(),client_id);
		ResultSet resultSet = statement.executeQuery(sql.toString());
		return resultSet;
	}
	/**
	 * 修改数据总量
	 * @throws SQLException
	 */
	public void updateRowCount(Connection conn,long id,Long totalRows,String databaseName,String tableName) throws SQLException {
		Statement statement = conn.createStatement();
		StringBuilder sql = new StringBuilder();
		sql.append("update "+databaseName+"."+tableName+" set total_rows ="+totalRows+", created_at=NOW()"+" where id = "+id);
		statement.executeUpdate(sql.toString());
	}

	/**
	 * 修改状态，设置该记录已经执行
	 * @param conn
	 * @param rowMap
	 * @param databaseName
	 * @param tableName
	 * @param trigger_type 0: 初始化,1: 运行中,2: 完成
	 * @throws SQLException
	 */
	public void updateRow(Connection conn,RowMap rowMap,String databaseName,String tableName,int trigger_type) throws SQLException {
		Statement statement = conn.createStatement();
		StringBuilder sql = new StringBuilder();
		sql.append("update "+databaseName+"."+tableName+" set is_trigger = "+trigger_type+" where id = "+rowMap.getData().get("id"));
		statement.executeUpdate(sql.toString());
	}
	public RowMap getInsertRow(long time,long serverId,ResultSet resultSet,Position position) throws SQLException {
		RowMap row = new RowMap("insert","maxwell","bootstrap",time,new ArrayList<String>(),position);
		row.setServerId(serverId);
		row.setXid(time);
		putData(resultSet, row,position,true);
		return row;
	}
	public RowMap getUpdateRow(long time,long serverId,ResultSet resultSet,Position position) throws SQLException {
		RowMap row = new RowMap("update","maxwell","bootstrap",time,new ArrayList<String>(),position);
		row.setServerId(serverId);
		row.setXid(time);
		putData(resultSet, row,position,false);
		return row;
	}

	/**
	 * 构建data
	 * execute : when it is true ,then init the totalRows
	 * @throws SQLException
	 */
	private void putData(ResultSet resultSet, RowMap row,Position position,boolean execute) throws SQLException {
		long id = resultSet.getLong("id");
		String database_name = resultSet.getString("database_name");
		String table_name = resultSet.getString("table_name");
		long is_complete = resultSet.getLong("is_complete");
		String started_at = resultSet.getString("started_at");
		String completed_at = resultSet.getString("completed_at");
		String where_clause = resultSet.getString("where_clause");
		String client_id = resultSet.getString("client_id");
		row.putData("id",id);
		row.putData("database_name",database_name);
		row.putData("table_name",table_name);
		row.putData("where_clause",where_clause);
		row.putData("is_complete",is_complete);
		row.putData("inserted_rows",0);
		row.putData("total_rows",0);
		row.putData("created_at",null);
		row.putData("started_at",started_at);
		row.putData("completed_at",completed_at);
		row.putData("binlog_file",null !=position ? (null!=position.getBinlogPosition() ? position.getBinlogPosition().getFile():null):null);
		row.putData("binlog_position",null !=position ? (null!=position.getBinlogPosition() ? position.getBinlogPosition().getOffset():0L):0L);
		row.putData("client_id",client_id);
		if(execute){
			Long totalRows = calculateRowCount(database_name,table_name,where_clause);
			if(0 < totalRows){
				Connection conn = this.connectionMaxwell;
				if(null == conn){
					return ;
				}
				updateRowCount(conn,id,totalRows,this.databaseName,"bootstrap");
			}
		}
	}
	private Long calculateRowCount( String db, String table, String whereClause) throws SQLException {
		Connection connection = this.connectionReplicate;
		if(null == connection){
			return -1L;
		}
		String sql = String.format("select count(*) from `%s`.%s", db, table);
		if ( whereClause != null && whereClause.length()>0) {
			sql += String.format(" where %s", whereClause);
		}
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		ResultSet resultSet = preparedStatement.executeQuery();
		resultSet.next();
		long rowCount = resultSet.getLong(1);
//		connection.close();
		return rowCount;
	}

//	public Event ANONYMOUSEvent(long time){
//		EventHeaderV4 eventHeaderV4 = getEventHeaderV4(1,EventType.XID,65,0,84547,time);
//		return new Event(eventHeaderV4, null);
//	}
	/**
	 * WRITE_ROWS事件
	 * @param time
	 * @return
	 */
//	public Event WRITE_ROWSEvent(long time) throws IOException {
//		EventHeaderV4 eventHeaderV4 = getEventHeaderV4(1,EventType.XID,88,0,84451,time);
//		WriteRowsEventData eventData = new WriteRowsEventData();
//		eventData.setTableId(110);
//		BitSet bitSet = new BitSet();
//		for(int i=0;i<=12;i++){
//			bitSet.set(i);
//		}
//		eventData.setIncludedColumns(bitSet);
//		List<Serializable[]> rows = new ArrayList<>();
//		rows.add(desCellInt(2));
//		rows.add(desCellStr("maxwell"));
//		eventData.setRows(rows);
//		return new Event(eventHeaderV4, eventData);
//	}
//	private Serializable[] desCellInt(int inputStream) throws IOException {
//		Serializable[] result = new Serializable[1];
//		result[0]=desInt(inputStream);
//		return  result;
//	}
//	private Serializable desInt(int inputStream) throws IOException {
//		return  inputStream;
//	}
//	private Serializable[] desCellStr(String inputStream) throws IOException {
//		Serializable[] result = new Serializable[1];
//		result[0]=desStr(inputStream);
//		return  result;
//	}
//	private Serializable desStr(String inputStream) throws IOException {
//		return  inputStream;
//	}
	/**
	 * TABLE_MAP事件
	 * @param time
	 * @return
	 */
//	public Event TABLE_MAPEvent(long time){
//		EventHeaderV4 eventHeaderV4 = getEventHeaderV4(1,EventType.XID,81,0,84363,time);
//		TableMapEventData eventData = new TableMapEventData();
//		eventData.setTableId(110);
//		eventData.setDatabase("maxwell");
//		eventData.setColumnTypes(new byte[]{3, 15, 15, 15, 1, 8, 8, 18, 18, 18, 15, 3, 15});
//		eventData.setColumnMetadata(new int[]{0, 765, 765, 255, 0, 0, 0, 0, 0, 0, 255, 0, 255});
//		BitSet bitSet = new BitSet();
//		bitSet.set(3);
//		bitSet.set(7);
//		bitSet.set(8);
//		bitSet.set(9);
//		bitSet.set(10);
//		bitSet.set(11);
//		eventData.setColumnNullability(bitSet);
//		return new Event(eventHeaderV4, eventData);
//	}
	/**
	 * xid事件
	 * @param time
	 * @return
	 */
//	public Event XidEvent(long time){
//		EventHeaderV4 eventHeaderV4 = getEventHeaderV4(1,EventType.XID,31,0,84482,time);
//		XidEventData xidEventData = new XidEventData();
//		xidEventData.setXid(4678);
//		return new Event(eventHeaderV4, xidEventData);
//	}
	/**
	 * Query事件
	 * @param time
	 * @return
	 */
//	public Event QueryEvent(long time){
//		EventHeaderV4 eventHeaderV4 = getEventHeaderV4(1,EventType.QUERY,73,0,84282,time);
//		QueryEventData queryEventData = new QueryEventData();
//		queryEventData.setThreadId(time);
//		queryEventData.setExecutionTime(0);
//		queryEventData.setErrorCode(0);
//		queryEventData.setDatabase("maxwell");
//		queryEventData.setSql("BEGIN");
//		return new Event(eventHeaderV4, queryEventData);
//	}
	/**
	 * 事件头
	 * @param serverId
	 * @param type
	 * @param length
	 * @param flags
	 * @param time
	 * @return
	 */
//	private EventHeaderV4 getEventHeaderV4(int serverId,EventType type,int length, int flags,long position,long time) {
//		EventHeaderV4 eventHeaderV4 = new EventHeaderV4();
//		eventHeaderV4.setServerId(serverId);
//		eventHeaderV4.setEventType(type);
//		eventHeaderV4.setEventLength(length);
//		eventHeaderV4.setFlags(flags);
//		eventHeaderV4.setNextPosition(position);
//		eventHeaderV4.setTimestamp(time);
//		return eventHeaderV4;
//	}
}
