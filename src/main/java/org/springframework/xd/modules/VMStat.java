/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.modules;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.xd.tuple.TupleBuilder;

/**
 * @author Patrick Peralta
 */
public class VMStat extends MessageProducerSupport {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private volatile String vmStatCommand = "vmstat -n 1";

	private final AtomicBoolean running = new AtomicBoolean(false);

	private final CountDownLatch shutdownLatch = new CountDownLatch(1);

	private final ExecutorService executorService =
			Executors.newSingleThreadExecutor(new CustomizableThreadFactory("vmstat"));

	public String getVmStatCommand() {
		return vmStatCommand;
	}

	public void setVmStatCommand(String vmStatCommand) {
		this.vmStatCommand = vmStatCommand;
	}

	@Override
	protected void doStart() {
		String os = System.getProperty("os.name");
		if (!os.toLowerCase().contains("linux")) {
			throw new UnsupportedOperationException("This module is only supported on Linux; OS detected: " + os);
		}

		if(running.compareAndSet(false, true)) {
			logger.info("Starting vmstat");
			executorService.submit(new VMStatExecutor());
			logger.info("Started vmstat");
		}
	}

	@Override
	protected void doStop() {
		if (running.compareAndSet(true, false)) {
			logger.info("Stopping vmstat");
			running.set(false);
			try {
				shutdownLatch.await(5, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				logger.warn("Interrupted while waiting for vmstat shutdown", e);
				Thread.currentThread().interrupt();
			}
			finally {
				executorService.shutdown();
				logger.info("Stopped vmstat");
			}
		}
	}


	public class VMStatExecutor implements Callable<Void> {

		@Override
		public Void call() throws Exception {
			logger.debug("Starting vmstat loop");
			ProcessBuilder builder = new ProcessBuilder();
			builder.redirectErrorStream(true);
			builder.command(getVmStatCommand().split("\\s"));

			Process process = null;
			String line = null;
			try {
				process = builder.start();

				InputStreamReader in = new InputStreamReader(new BufferedInputStream(process.getInputStream()));
				BufferedReader reader = new BufferedReader(in);

				while (running.get() && (line = reader.readLine()) != null) {
					TupleBuilder tupleBuilder = TupleBuilder.tuple();
					StringTokenizer tokenizer = new StringTokenizer(line);

					try {
						tupleBuilder.put("waitingProcessCount", Integer.parseInt(tokenizer.nextToken()))
								.put("sleepingProcessCount", Integer.parseInt(tokenizer.nextToken()))
								.put("virtualMemoryUsage", Long.parseLong(tokenizer.nextToken()))
								.put("freeMemory", Long.parseLong(tokenizer.nextToken()))
								.put("bufferMemory", Long.parseLong(tokenizer.nextToken()))
								.put("cacheMemory", Long.parseLong(tokenizer.nextToken()))
								.put("swapIn", Long.parseLong(tokenizer.nextToken()))
								.put("swapOut", Long.parseLong(tokenizer.nextToken()))
								.put("bytesIn", Long.parseLong(tokenizer.nextToken()))
								.put("bytesOut", Long.parseLong(tokenizer.nextToken()))
								.put("interruptsPerSecond", Long.parseLong(tokenizer.nextToken()))
								.put("contextSwitchesPerSecond", Long.parseLong(tokenizer.nextToken()))
								.put("userCpu", Long.parseLong(tokenizer.nextToken()))
								.put("kernelCpu", Long.parseLong(tokenizer.nextToken()))
								.put("idleCpu", Long.parseLong(tokenizer.nextToken()));
					}
					catch (Exception e) {
						// most likely cause is parsing a header line; discard and continue
						tupleBuilder = null;
					}

					if (tupleBuilder != null) {
						sendMessage(MessageBuilder.withPayload(tupleBuilder.build()).build());
					}
				}
			}
			catch (IOException e) {
				logger.error("Unhandled exception", e);
				running.set(false);
			}
			finally {
				if (process != null) {
					process.destroy();
				}
				shutdownLatch.countDown();
				logger.debug("Stopping vmstat loop");
				logger.trace("running: {}, line: {}", running, line);
			}

			return null;
		}
	}

}
