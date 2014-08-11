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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.xd.tuple.Tuple;
import org.springframework.xd.tuple.TupleBuilder;

/**
 * @author Patrick Peralta
 */
public class VMStat extends MessageProducerSupport {

	private volatile String vmstatCommand = "vmstat -n 1";

	private final ExecutorService executorService = Executors.newSingleThreadExecutor();

	public class VMStatExecutor implements Callable<Void> {

		@Override
		public Void call() throws Exception {
//			while (true) {
//				TupleBuilder tupleBuilder = TupleBuilder.tuple();
//				tupleBuilder.put("hello", "world");
//				sendMessage(MessageBuilder.withPayload(tupleBuilder.build()).build());
//
//				try {
//					Thread.sleep(1000);
//				}
//				catch (InterruptedException e) {
//					throw new RuntimeException(e);
//				}
//			}

			ProcessBuilder builder = new ProcessBuilder();
			builder.redirectErrorStream(true);
			builder.command(vmstatCommand.split("\\s"));

			Process process = null;
			try {
				process = builder.start();

				InputStreamReader in = new InputStreamReader(new BufferedInputStream(process.getInputStream()));
				BufferedReader reader = new BufferedReader(in);
				String line;

				while ((line = reader.readLine()) != null) {
					TupleBuilder tupleBuilder = TupleBuilder.tuple();
//					StringTokenizer tokenizer = new StringTokenizer(line);

					try {
						tupleBuilder.put("ls", line);
//						tupleBuilder.put("waitingProcessCount", Integer.parseInt(tokenizer.nextToken()))
//								.put("sleepingProcessCount", Integer.parseInt(tokenizer.nextToken()))
//								.put("virtualMemoryUsage", Long.parseLong(tokenizer.nextToken()))
//								.put("freeMemory", Long.parseLong(tokenizer.nextToken()))
//								.put("bufferMemory", Long.parseLong(tokenizer.nextToken()))
//								.put("cacheMemory", Long.parseLong(tokenizer.nextToken()))
//								.put("swapIn", Long.parseLong(tokenizer.nextToken()))
//								.put("swapOut", Long.parseLong(tokenizer.nextToken()))
//								.put("bytesIn", Long.parseLong(tokenizer.nextToken()))
//								.put("bytesOut", Long.parseLong(tokenizer.nextToken()))
//								.put("interruptsPerSecond", Long.parseLong(tokenizer.nextToken()))
//								.put("contextSwitchesPerSecond", Long.parseLong(tokenizer.nextToken()))
//								.put("userCpu", Long.parseLong(tokenizer.nextToken()))
//								.put("kernelCpu", Long.parseLong(tokenizer.nextToken()))
//								.put("idleCpu", Long.parseLong(tokenizer.nextToken()));

						sendMessage(MessageBuilder.withPayload(tupleBuilder.build()).build());
					}
					catch (Exception e) {
						// most likely cause is parsing a header line; discard and continue
					}
				}
			}
			catch (IOException e) {
			}
			finally {
				if (process != null) {
					process.destroy();
				}
			}

			return null;
		}
	}

	@Override
	protected void doStart() {
		executorService.submit(new VMStatExecutor());
	}

	@Override
	protected void doStop() {
		executorService.shutdown();
	}
}
