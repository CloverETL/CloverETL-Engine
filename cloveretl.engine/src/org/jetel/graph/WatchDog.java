/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
package org.jetel.graph;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.util.StringUtils;

/**
 *  Description of the Class
 *
 * @author      dpavlis
 * @since       July 29, 2002
 * @revision    $Revision$
 */
class WatchDog extends Thread {
	private int trackingInterval;
	private int watchDogStatus;
	private TransformationGraph graph;
	private Phase[] phases;
	private Phase currentPhase;
	private int currentPhaseNum;
	private Runtime javaRuntime;
	private int freeMemoryStampKB;

	/**  Description of the Field */
	public final static int WATCH_DOG_STATUS_READY = 0;
	/**  Description of the Field */
	public final static int WATCH_DOG_STATUS_ERROR = -1;
	/**  Description of the Field */
	public final static int WATCH_DOG_STATUS_RUNNING = 1;
	/**  Description of the Field */
	public final static int WATCH_DOG_STATUS_FINISHED_OK = 2;
    
    public final static String TRACKING_LOGGER_NAME = "Tracking";

    static Log logger = LogFactory.getLog(WatchDog.class);
    


	/**
	 *Constructor for the WatchDog object
	 *
	 * @param  graph   Description of the Parameter
	 * @param  phases  Description of the Parameter
	 * @since          September 02, 2003
	 */
	WatchDog(TransformationGraph graph, Phase[] phases) {
		super("WatchDog");
		trackingInterval = Defaults.WatchDog.DEFAULT_WATCHDOG_TRACKING_INTERVAL;
		setDaemon(true);
		this.graph = graph;
		this.phases = phases;
		currentPhase = null;
		watchDogStatus = WATCH_DOG_STATUS_READY;
		javaRuntime = Runtime.getRuntime();
		freeMemoryStampKB = (int) javaRuntime.freeMemory()/1024;
	}


	/**
	 *Constructor for the WatchDog object
	 *
	 * @param  out       Description of Parameter
	 * @param  tracking  Description of Parameter
	 * @param  graph     Description of the Parameter
	 * @param  phases    Description of the Parameter
	 * @since            September 02, 2003
	 */
	WatchDog(TransformationGraph graph, Phase[] phases, int tracking) {
		this(graph,phases);
		trackingInterval = tracking;
	}


	/**  Main processing method for the WatchDog object */
	public void run() {
		watchDogStatus = WATCH_DOG_STATUS_RUNNING;
		logger.info("Thread started.");
		logger.info("Running on " + javaRuntime.availableProcessors() + " CPU(s)"
			+ " max available memory for JVM " + javaRuntime.freeMemory() / 1024 + " KB");
		// renice - lower the priority
		setPriority(Thread.MIN_PRIORITY);
		
		for (currentPhaseNum = 0; currentPhaseNum < phases.length; currentPhaseNum++) {
			if (!runPhase(phases[currentPhaseNum])) {
				watchDogStatus = WATCH_DOG_STATUS_ERROR;
				logger.error("!!! Phase finished with error - stopping graph run !!!");
				return;
			}
			// force running of garbage collector
			logger.info("Forcing garbage collection ...");
			javaRuntime.runFinalization();
			javaRuntime.gc();
		}

		watchDogStatus = WATCH_DOG_STATUS_FINISHED_OK;
		printPhasesSummary();
	}


	/**
	 * Execute transformation - start-up all Nodes & watch them running
	 *
	 * @param  phase      Description of the Parameter
	 * @param  leafNodes  Description of the Parameter
	 * @return            Description of the Return Value
	 * @since             July 29, 2002
	 */
	public boolean watch(Phase phase, List leafNodes) {
		int phaseMemUtilizationMaxKB;
		int usedMemoryKB;
		Iterator leafNodesIterator;
		Iterator nodesIterator;
		Node node;
		int ticker = Defaults.WatchDog.NUMBER_OF_TICKS_BETWEEN_STATUS_CHECKS;
		long lastTimestamp;
		long currentTimestamp;
		long startTimestamp;

		//get current memory utilization
		phaseMemUtilizationMaxKB=freeMemoryStampKB - (int) javaRuntime.freeMemory()/1024;
		//let's take timestamp so we can measure processing
		startTimestamp = lastTimestamp = System.currentTimeMillis();
		// entering the loop awaiting completion of work by all leaf nodes
		while (true) {
			if (leafNodes.isEmpty()) {
				phase.setPhaseMemUtilization(phaseMemUtilizationMaxKB);
				phase.setPhaseExecTime((int) (System.currentTimeMillis() - startTimestamp));
				logger.info("Execution of phase [" + phase.getPhaseNum() + "] successfully finished - elapsed time(sec): "
						+ phase.getPhaseExecTime() / 1000);
				//printProcessingStatus(phase.getNodes().iterator(), phase.getPhaseNum());
                new Thread(new PrintTracking(phase.getNodes().iterator(), phase.getPhaseNum()),"PrintTracking").run();
				return true;
				// nothing else to do in this phase
			}
			leafNodesIterator = leafNodes.iterator();
			while (leafNodesIterator.hasNext()) {
				node = (Node) leafNodesIterator.next();
				// is this Node still alive - ? doing something
				if (!node.getNodeThread().isAlive()) {
					leafNodesIterator.remove();
				}
			}
			// from time to time perform some task
			if ((ticker--) == 0) {
				// reinitialize ticker
				ticker = Defaults.WatchDog.NUMBER_OF_TICKS_BETWEEN_STATUS_CHECKS;
				// get memory usage mark
				usedMemoryKB = freeMemoryStampKB - (int) javaRuntime.freeMemory()/1024;
				if (phaseMemUtilizationMaxKB < usedMemoryKB) {
					phaseMemUtilizationMaxKB = usedMemoryKB;
				}
				// check that no Node finished with some fatal error
				nodesIterator = phase.getNodes().iterator();
				while (nodesIterator.hasNext()) {
					node = (Node) nodesIterator.next();
					if ((!node.getNodeThread().isAlive()) && (node.getResultCode() != Node.RESULT_OK)) {
						logger.fatal("!!! Fatal Error !!! - graph execution is aborting");
						logger.error("Node " + node.getId() + " finished with fatal error: " + node.getResultMsg());
						watchDogStatus = WATCH_DOG_STATUS_ERROR;
						abort();
						//printProcessingStatus(phase.getNodes().iterator(), phase.getPhaseNum());
                        new Thread(new PrintTracking(phase.getNodes().iterator(), phase.getPhaseNum()),"PrintTracking").run();
						return false;
					}
				}
			}
			// Display processing status, if it is time
			currentTimestamp = System.currentTimeMillis();
			if ((currentTimestamp - lastTimestamp) >= trackingInterval) {
				//printProcessingStatus(phase.getNodes().iterator(), phase.getPhaseNum());
                new Thread(new PrintTracking(phase.getNodes().iterator(), phase.getPhaseNum()),"PrintTracking").run();
				lastTimestamp = currentTimestamp;
			}
			// rest for some time
			try {
				sleep(Defaults.WatchDog.WATCHDOG_SLEEP_INTERVAL);
			} catch (InterruptedException ex) {
				watchDogStatus = WATCH_DOG_STATUS_ERROR;
				return false;
			}
		}
	}


	/**
	 *  Gets the Status of the WatchDog
	 *
	 * @return	0 READY, -1 ERROR, 1 RUNNING, 2 FINISHED OK    
	 * @since     July 30, 2002
	 */
	public int getStatus() {
		return watchDogStatus;
	}


	/**
	 *  Outputs basic LOG information about graph processing
	 *
	 * @param  iterator  Description of Parameter
	 * @param  phaseNo   Description of the Parameter
	 * @since            July 30, 2002
	 */
    /*
	void printProcessingStatus(Iterator iterator, int phaseNo) {
		int i;
		int recCount;
		Node node;
        trackingLogger.info("---------------------** Start of tracking Log for phase [" + phaseNo + "] **-------------------");
		// France is here just to get 24hour time format
        trackingLogger.info("Time: "
			+ DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM, Locale.FRANCE).
				format(Calendar.getInstance().getTime()));
        trackingLogger.info("Node                        Status         Port                          #Records");
        trackingLogger.info("---------------------------------------------------------------------------------");
		while (iterator.hasNext()) {
			node = (Node) iterator.next();
			Object nodeInfo[] = {node.getID(), node.getStatus()};
			int nodeSizes[] = {-28, -15};
            trackingLogger.info(StringUtils.formatString(nodeInfo, nodeSizes));
			//in ports
			i = 0;
			while ((recCount = node.getRecordCount(Node.INPUT_PORT, i)) != -1) {
				Object portInfo[] = {"In:", Integer.toString(i), Integer.toString(recCount)};
				int portSizes[] = {47, -2, 32};
                trackingLogger.info(StringUtils.formatString(portInfo, portSizes));
				i++;
			}
			//out ports
			i = 0;
			while ((recCount = node.getRecordCount(Node.OUTPUT_PORT, i)) != -1) {
				Object portInfo[] = {"Out:", Integer.toString(i), Integer.toString(recCount)};
				int portSizes[] = {47, -2, 32};
                trackingLogger.info(StringUtils.formatString(portInfo, portSizes));
				i++;
			}
		}
        trackingLogger.info("---------------------------------** End of Log **--------------------------------");
	}
*/

	/**  Outputs summary info about executed phases */
	void printPhasesSummary() {
		logger.info("-----------------------** Summary of Phases execution **---------------------");
		logger.info("Phase#            Finished Status         RunTime(sec)    MemoryAllocation(KB)");
		for (int i = 0; i < phases.length; i++) {
			Object nodeInfo[] = {new Integer(phases[i].getPhaseNum()), new Integer(0),
					new Integer(phases[i].getPhaseExecTime()/1000), new Integer(phases[i].getPhaseMemUtilization())};
			int nodeSizes[] = {-18, -24, 12, 18};
			logger.info(StringUtils.formatString(nodeInfo, nodeSizes));
		}
		logger.info("------------------------------** End of Summary **---------------------------");
	}


	/**
	 * aborts execution of current phase
	 *
	 * @since    July 29, 2002
	 */
	void abort() {
		Iterator iterator = currentPhase.getNodes().iterator();
		Node node;

		// iterate through all the nodes and stop them
		while (iterator.hasNext()) {
			node = (Node) iterator.next();
			node.abort();
			logger.warn("Interrupted node: "
				+ node.getId());
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodesIterator  Description of Parameter
	 * @param  leafNodesList  Description of Parameter
	 * @since                 July 31, 2002
	 */
	private void startUpNodes(Iterator nodesIterator, List leafNodesList) {
		Node node;
		while (nodesIterator.hasNext()) {
			node = (Node) nodesIterator.next();
            Thread nodeThread = new Thread(node, node.getId());
//          // this thread is daemon - won't live if main thread ends
            nodeThread.setDaemon(true);
            node.setNodeThread(nodeThread);
			nodeThread.start();
			if (node.isLeaf() || node.isPhaseLeaf()) {
				leafNodesList.add(node);
			}
			logger.debug(node.getId()+ " ... started");
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  phase  Description of the Parameter
	 * @return        Description of the Return Value
	 */
	private boolean runPhase(Phase phase) {
		boolean result;
		currentPhase = phase;
		List leafNodes = new LinkedList();
		//phase.checkConfig();
		// init phase
		if (!phase.init()) {
			// something went wrong !
			return false;
		}
		logger.info("Starting up all nodes in phase [" + phase.getPhaseNum() + "]");
		startUpNodes(phase.getNodes().iterator(), leafNodes);
		logger.info("Sucessfully started all nodes in phase!");
		// watch running nodes in phase
		result = watch(phase, leafNodes);
        // check how nodes in phase finished
        Node node;
        for (Iterator i=phase.getNodes().iterator();i.hasNext();){
            node=(Node)i.next();
            // was there an uncaught error ?
            if (node.getResultCode()==Node.RESULT_ERROR || node.getResultCode()==Node.RESULT_FATAL_ERROR){
               result=false;
               logger.error("in node "+node.getId()+" : "+node.getResultMsg());
            }
        }
		//end of phase, destroy it
		phase.destroy();
		return result;
	}

	/*
	 *  private void initialize(){
	 *  }
	 */
	/**
	 * @return Returns the currentPhaseNum - which phase is currently 
	 * beeing executed
	 */
	public int getCurrentPhaseNum() {
		return currentPhaseNum;
	}
	
	/**
	 * @return Returns the trackingInterval - how frequently is the status printed (in ms).
	 */
	public int getTrackingInterval() {
		return trackingInterval;
	}
	
	/**
	 * @param trackingInterval How frequently print the processing status (in ms).
	 */
	public void setTrackingInterval(int trackingInterval) {
		this.trackingInterval = trackingInterval;
	}
    
	class PrintTracking implements Runnable{
	    Iterator iterator;
        int phaseNo;
        Log trackingLogger;
        
        PrintTracking(Iterator iterator, int phaseNo){
            this.iterator=iterator;
            this.phaseNo=phaseNo;
            trackingLogger= LogFactory.getLog(TRACKING_LOGGER_NAME);
        }
        
        public void run(){
            printProcessingStatus();
        }
        
        /**
         *  Outputs basic LOG information about graph processing
         *
         * @param  iterator  Description of Parameter
         * @param  phaseNo   Description of the Parameter
         * @since            July 30, 2002
         */
        private void printProcessingStatus() {
            int i;
            int recCount;
            Node node;
            trackingLogger.info("---------------------** Start of tracking Log for phase [" + phaseNo + "] **-------------------");
            // France is here just to get 24hour time format
            trackingLogger.info("Time: "
                + DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM, Locale.FRANCE).
                    format(Calendar.getInstance().getTime()));
            trackingLogger.info("Node                        Status         Port                          #Records");
            trackingLogger.info("---------------------------------------------------------------------------------");
            while (iterator.hasNext()) {
                node = (Node) iterator.next();
                Object nodeInfo[] = {node.getId(), node.getStatus()};
                int nodeSizes[] = {-28, -15};
                trackingLogger.info(StringUtils.formatString(nodeInfo, nodeSizes));
                //in ports
                i = 0;
                while ((recCount = node.getRecordCount(Node.INPUT_PORT, i)) != -1) {
                    Object portInfo[] = {"In:", Integer.toString(i), Integer.toString(recCount)};
                    int portSizes[] = {47, -2, 32};
                    trackingLogger.info(StringUtils.formatString(portInfo, portSizes));
                    i++;
                }
                //out ports
                i = 0;
                while ((recCount = node.getRecordCount(Node.OUTPUT_PORT, i)) != -1) {
                    Object portInfo[] = {"Out:", Integer.toString(i), Integer.toString(recCount)};
                    int portSizes[] = {47, -2, 32};
                    trackingLogger.info(StringUtils.formatString(portInfo, portSizes));
                    i++;
                }
            }
            trackingLogger.info("---------------------------------** End of Log **--------------------------------");
        }

    }
    
}

