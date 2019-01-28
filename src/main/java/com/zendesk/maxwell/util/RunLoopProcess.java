package com.zendesk.maxwell.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

abstract public class RunLoopProcess implements StoppableTask {
	private static final Logger LOGGER = LoggerFactory.getLogger(RunLoopProcess.class);
	protected volatile StoppableTaskState taskState;
	private Thread thread;

	public RunLoopProcess() {
		this.taskState = new StoppableTaskState(this.getClass().getName());
	}

	public void requestStop() {
		this.taskState.requestStop();
	}

	public void awaitStop(Long timeout) throws TimeoutException {
		this.taskState.awaitStop(thread, timeout);
	}

	public void runLoop() throws Exception {
		this.thread = Thread.currentThread();
		this.beforeStart();

		try {
			while (this.taskState.isRunning()) {
//				if(!"main".equals(thread.getName()))
//					LOGGER.info("RunLoopProcess -->runLoop 调用了,thread:{}",thread.getName());
				work();
			}
		} finally {
			this.beforeStop();
			this.taskState.stopped();
		}
	}

	protected abstract void work() throws Exception;
//	protected abstract void bootstrapEvent() throws Exception;
	protected void beforeStart() throws Exception { }
	protected void beforeStop() throws Exception { }
}
