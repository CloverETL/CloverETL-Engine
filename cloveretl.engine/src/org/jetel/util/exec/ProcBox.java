/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.util.exec;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.JetelException;

/**
 * Class for running OS processes. It's main purpose is to provide convenient mechanism
 * for supplying process' input and processing it's output.
 * <p> An instance is composed of
 * four components:<ul>
 * <li> externally created process,</li>
 * <li> a producer which is able to supply one piece of input data in one step,</li>
 * <li> a consumer processing one piece of output data in one step,</li>
 * <li> and an error consume processing one piece of error output in one step</li>
 * </ul>
 * <p> Exact meaning of phrase "one piece of data" is not defined and it may differ across
 * various consumers/producers.
 * <p> Producer and both consumers are run in separate threads which repeatedly and concurrently call their
 * produce/consume method.
 *
 * NOTE: implementation with {@link ProducerConsumerExecutor} not tested, found in SVN revision 5242
 *
 * @see ProducerConsumerExecutor
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 10/17/06 
 */
public class ProcBox {
	/**
	 * process to run
	 */
	private Process proc;
	/**
	 * producer thread
	 */
	private ProducerThread producer;
	/**
	 * consumer thread
	 */
	private ConsumerThread consumer;
	/**
	 * error consumer thread
	 */
	private ConsumerThread errConsumer;

	/**
	 * output stream
	 */
	private OutputStream outStream;
	/**
	 * input stream
	 */
	private InputStream inStream;
	/**
	 * error output stream
	 */
	private InputStream errStream;

	static Log logger = LogFactory.getLog(ProcBox.class);

	/**
	 * Sole ctor. Creates an instance from its four components. Starts producer/consumer threads. 
	 * @param proc Running process.
	 * @param producer 
	 * @param consumer
	 * @param errConsumer
	 */
	public ProcBox(Process proc, DataProducer producer, DataConsumer consumer, DataConsumer errConsumer) {
		this.proc = proc;
		outStream = new BufferedOutputStream(proc.getOutputStream());
		inStream = new BufferedInputStream(proc.getInputStream());
		errStream = new BufferedInputStream(proc.getErrorStream());
		if (producer == null) {
			this.producer = null;
		} else {
			// create producer thread
			this.producer = new ProducerThread(producer, outStream);
		}
		// create consumer thread
		if (consumer == null) {
			this.consumer = new ConsumerThread(new WasteDataConsumer(), inStream);
		} else {
			this.consumer = new ConsumerThread(consumer, inStream);
		}
		// create error consumer thread
		if (errConsumer == null) {
			this.errConsumer = new ConsumerThread(new WasteDataConsumer(), errStream);			
		} else {
			this.errConsumer = new ConsumerThread(errConsumer, errStream);			
		}

		// start producer/consumers
		if (this.producer != null) {
			this.producer.start();
		}
		this.consumer.start();
		this.errConsumer.start();
	}
	
	/**
	 * Joins the process and all slave threads. 
	 * @return Return value of finished process
	 * @throws InterruptedException
	 */
	public int join()
	throws InterruptedException {
		if (producer != null) {
			producer.join();
		}
		try {
			outStream.close();
		} catch (IOException e) {
			logger.warn("Cannot close process' input", e);
		}
		int retval = proc.waitFor();
		consumer.join();
		try {
			inStream.close();
		} catch (IOException e) {
			logger.warn("Cannot close process' output", e);
		}
		errConsumer.join();
		try {
			errStream.close();
		} catch (IOException e) {
			logger.warn("Cannot close process' error output", e);
		}
		return retval;
	}
	
	/**
	 * @return list of all inner threads - producer, consumer and error consumer thread
	 */
	public List<Thread> getChildThreads() {
		List<Thread> childThreads = new ArrayList<Thread>();
		
		if (producer != null) {
			childThreads.add(producer);
		}
		childThreads.add(consumer);
		childThreads.add(errConsumer);
		
		return childThreads;
	}
	
	/**
	 * Nomen omen.
	 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
	 *
	 */
	private static class ProducerThread extends Thread {
		private boolean runIt = true;
		private DataProducer producer;
		private OutputStream stream;

		/**
		 * Sole ctor. Creates thread which uses specified producer to supply data to specified stream, which
		 * is supposed to be connected to process' input.
		 * @param producer
		 * @param stream
		 */
		public ProducerThread(DataProducer producer, OutputStream stream) {
			super(Thread.currentThread().getName() + ".ProducerThread");
			this.producer = producer;
			this.stream = stream;
		}
		
		/**
		 * @see java.lang.Thread#run()
		 */
		@Override
		public void run() {
			try {
				producer.setOutput(stream);
				while (runIt && producer.produce());
				producer.close();
			} catch (JetelException e) {
				logger.error("Data producer failed", e);
			} catch (IOException e) {
				logger.error("Data producer failed: output stream cannot be closed.", e);
			}
		}
	}

	/**
	 * Nomen omen.
	 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
	 *
	 */
	private static class ConsumerThread extends Thread {
		private boolean runIt = true;
		private DataConsumer consumer;
		private InputStream stream;

		/**
		 * Sole ctor. Creates thread which uses specified consumer to process data from specified stream, which
		 * is supposed to be connected either to process' output or to process' error output.
		 * @param consumer
		 * @param stream
		 */
		public ConsumerThread(DataConsumer consumer, InputStream stream) {
			super(Thread.currentThread().getName() + ".ConsumerThread");
			this.consumer = consumer;
			this.stream = stream;			
		}
		
		/**
		 * @see java.lan.Thread#run()
		 */
		@Override
		public void run() {
			try {
				consumer.setInput(stream);
				while (runIt && consumer.consume());
				consumer.close();
			} catch (JetelException e) {
				logger.error("Data consumer failed", e);
			} catch (IOException e) {
				logger.error("Data consumer failed: input stream cannot be closed.", e);
			}
		}
	}

	/**
	 * A consumer which is used whenever user doesn't specify consumer or error consumer.
	 * It reads data from input stream and instantly discards them.
	 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
	 *
	 */
	private static class WasteDataConsumer implements  DataConsumer {
		private InputStream stream;
		private byte buf[] = new byte[Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE];
		
		@Override
		public void setInput(InputStream stream) {
			this.stream = stream;
		}
		@Override
		public boolean consume() throws JetelException {
			try {
				return stream.read(buf) > -1;
			} catch (IOException e) {
				throw new JetelException("Error while reading input buffer", e);
			}
		}
		@Override
		public void close() {
		}
	}
	
}
