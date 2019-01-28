package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BinlogConnectorLifecycleListener implements BinaryLogClient.LifecycleListener {
	private static final Logger LOGGER = LoggerFactory.getLogger(BinlogConnectorLifecycleListener.class);

	@Override
	public void onConnect(BinaryLogClient client) {
		LOGGER.info("LifecycleListener onConnect -->Binlog connected.");
	};

	@Override
	public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
		LOGGER.warn("LifecycleListener Communication failure.", ex);
	}

	@Override
	public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
		LOGGER.warn("LifecycleListener Event deserialization failure.", ex);
		LOGGER.warn("LifecycleListener cause: ", ex.getCause());
	}

	@Override
	public void onDisconnect(BinaryLogClient client) {
		LOGGER.info("LifecycleListener onDisconnect Binlog disconnected.");
	}
}

