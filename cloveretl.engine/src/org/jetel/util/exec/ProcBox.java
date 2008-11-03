/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.util.exec;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 10/17/06 
 */
public class ProcBox {
	
	private ProducerConsumerExecutor executor;
	
	static Log logger = LogFactory.getLog(ProcBox.class);

	/**
	 * Sole ctor. Creates an instance from its four components. Starts producer/consumer threads. 
	 * @param proc Running process.
	 * @param producer 
	 * @param consumer
	 * @param errConsumer
	 */
	public ProcBox(Process proc, DataProducer producer, DataConsumer consumer, DataConsumer errConsumer) {
		this.executor = new ProducerConsumerExecutor();
		executor.addProcess(proc);
		
		if (producer != null) {
			executor.addProducer(producer, new BufferedOutputStream(proc.getOutputStream()));
		}
		
		executor.addConsumer(consumer, new BufferedInputStream(proc.getInputStream()));
		
		executor.addConsumer(errConsumer, new BufferedInputStream(proc.getErrorStream()));			

		executor.start();
	}
	
	/**
	 * Joins the process and all slave threads. 
	 * @return Return value of finished process
	 * @throws InterruptedException
	 */
	public int join()
	throws InterruptedException {
		return executor.join();
	}
	
	
	/**
     * This method determine platform type.
     * 
     * @return          true if the platform is Windows else false
     * @since 23.8.2007
     */
	public static boolean isWindowsPlatform() {
		return System.getProperty("os.name").contains("Windows");
	} 
}
