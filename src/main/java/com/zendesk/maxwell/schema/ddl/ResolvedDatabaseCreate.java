package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResolvedDatabaseCreate extends ResolvedSchemaChange {
	static final Logger LOGGER = LoggerFactory.getLogger(ResolvedDatabaseCreate.class);
	public String database;
	public String charset;

	public ResolvedDatabaseCreate() { }
	public ResolvedDatabaseCreate(String database, String charset) {
		this.database = database;
		this.charset = charset;
	}

	@Override
	public void apply(Schema schema) throws InvalidSchemaError {
		LOGGER.info("ResolvedDatabaseCreate-->apply schema.getDatabaseNames:{},database:{}",schema.getDatabaseNames(),this.database);
		if ( schema.hasDatabase(database) )
			return;
//			throw new InvalidSchemaError("Unexpectedly asked to create existing database " + database);

		schema.addDatabase(new Database(database, charset));
	}

	@Override
	public String databaseName() {
		return database;
	}

	@Override
	public String tableName() {
		return null;
	}
}
