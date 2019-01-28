package com.zendesk.maxwell.producer;

import com.codahale.metrics.Gauge;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public abstract class AbstractAsyncProducer extends AbstractProducer {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAsyncProducer.class);
	public class CallbackCompleter {
		private InflightMessageList inflightMessages;
		private final MaxwellContext context;
		private final Position position;
		private final boolean isTXCommit;

		public CallbackCompleter(InflightMessageList inflightMessages, Position position, boolean isTXCommit, MaxwellContext context) {
			this.inflightMessages = inflightMessages;
			this.context = context;
			this.position = position;
			this.isTXCommit = isTXCommit;
		}

		public void markCompleted() {
			if(isTXCommit) {
				InflightMessageList.InflightMessage message = inflightMessages.completeMessage(position);

				if (message != null) {
					context.setPosition(message.position);
					long currentTime = System.currentTimeMillis();
					messagePublishTimer.update(currentTime - message.sendTimeMS, TimeUnit.MILLISECONDS);
					messageLatencyTimer.update(Math.max(0L, currentTime - message.eventTimeMS - 500L), TimeUnit.MILLISECONDS);
				}
			}
		}
	}

	private InflightMessageList inflightMessages;

	public AbstractAsyncProducer(MaxwellContext context) {
		super(context);

		this.inflightMessages = new InflightMessageList(context);

		Metrics metrics = context.getMetrics();
		String gaugeName = metrics.metricName("inflightmessages", "count");
		metrics.register(gaugeName, (Gauge<Long>) () -> (long) inflightMessages.size());
	}

	public abstract void sendAsync(RowMap r, CallbackCompleter cc) throws Exception;

	@Override
	public final void push(RowMap r) throws Exception {
		Position position = r.getNextPosition();
		// Rows that do not get sent to a target will be automatically marked as complete.
		// We will attempt to commit a checkpoint up to the current row.
		if(!r.shouldOutput(outputConfig)) {
			inflightMessages.addMessage(position, r.getTimestampMillis());

			InflightMessageList.InflightMessage completed = inflightMessages.completeMessage(position);
			if(completed != null) {
				context.setPosition(completed.position);
			}
			return;
		}

		if(r.isTXCommit()) {
			inflightMessages.addMessage(position, r.getTimestampMillis());
		}

		CallbackCompleter cc = new CallbackCompleter(inflightMessages, position, r.isTXCommit(), context);
//		LOGGER.info("AbstractAsyncProducer push:{},ThreadThread:{}",r,Thread.currentThread().getName());
		sendAsync(r, cc);
	}
}
