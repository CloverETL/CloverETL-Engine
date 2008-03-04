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
package org.jetel.graph.runtime;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.MDC;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Edge;
import org.jetel.graph.GraphElement;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Phase;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.TrackingDetail.PortType;
import org.jetel.util.primitive.DuplicateKeyMap;
import org.jetel.util.string.StringUtils;


/**
 *  Description of the Class
 *
 * @author      dpavlis
 * @since       July 29, 2002
 * @revision    $Revision$
 */
public class WatchDog implements Callable<Result>, CloverPost {
    private IThreadManager threadManager;
    private int trackingInterval;
	private Result watchDogStatus;
	private TransformationGraph graph;
	private Phase currentPhase;
	private int currentPhaseNum;
	private Runtime javaRuntime;
    private MemoryMXBean memMXB;
    private ThreadMXBean threadMXB;
    private BlockingQueue <Message> inMsgQueue;
    private DuplicateKeyMap outMsgMap;
    private Throwable causeException;
    private GraphElement causeGraphElement;
    private CloverJMX mbean;
    private volatile boolean runIt;
    private boolean threadCpuTimeIsSupported;
    private boolean provideJMX=true;
    private boolean finishJMX = true; //whether the JMX mbean should be unregistered on the graph finish 
    private GraphRuntimeContext runtimeContext;
    
    private PrintTracking printTracking;

    public final static String TRACKING_LOGGER_NAME = "Tracking";
    public final static String MBEAN_NAME_PREFIX = "CLOVERJMX_";
    public final static int WAITTIME_FOR_STOP_SIGNAL = 5000; //milliseconds
    private int[] _MSG_LOCK=new int[0];
    
    static Log logger = LogFactory.getLog(WatchDog.class);
    
    static private MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    private ObjectName jmxObjectName;


	/**
	 *Constructor for the WatchDog object
	 *
	 * @param  graph   Description of the Parameter
	 * @param  phases  Description of the Parameter
	 * @since          September 02, 2003
	 */
	public WatchDog(IThreadManager threadManager, TransformationGraph graph, GraphRuntimeContext runtimeContext) {
		this.threadManager = threadManager;
		graph.setWatchDog(this);
		this.graph = graph;
		this.runtimeContext = runtimeContext;
		currentPhase = null;
		watchDogStatus = Result.READY;
		javaRuntime = Runtime.getRuntime();
        memMXB=ManagementFactory.getMemoryMXBean();
        threadMXB= ManagementFactory.getThreadMXBean();
        threadCpuTimeIsSupported = threadMXB.isThreadCpuTimeSupported();
        
        inMsgQueue=new PriorityBlockingQueue<Message>();
        outMsgMap=new DuplicateKeyMap(Collections.synchronizedMap(new HashMap()));
        trackingInterval=runtimeContext.getTrackingInterval();
        
        //is JMX turned on?
        provideJMX = runtimeContext.useJMX();
        
        //passes a password from context to the running graph
        graph.setPassword(runtimeContext.getPassword());
	}

	public void startUpJMX() {
       	mbean=registerTrackingMBean(provideJMX);
	}
	
	public void finishJMX() {
        // wait to get JMX chance to propagate the message
		if (mbean.hasClients()) {
			long timestamp = System.currentTimeMillis();
			try {
				while (runIt
						&& (System.currentTimeMillis() - timestamp) < WAITTIME_FOR_STOP_SIGNAL)
					Thread.sleep(250);
			} catch (InterruptedException ex) {
				// do nothing
			}
		}
        
		printPhasesSummary();
		
		if(provideJMX) {
			try {
				mbs.unregisterMBean(jmxObjectName);
			} catch (Exception e) {
				logger.error("JMX error - ObjectName cannot be unregistered.", e);
			}
		}
	}
	
	/**  Main processing method for the WatchDog object */
	public Result call() {
		MDC.put("runId", runtimeContext.getRunId());
		
		long startTimestamp = System.currentTimeMillis();
		
        if (runtimeContext.isVerboseMode()) {
            // this can be called only after graph.init()
            graph.dumpGraphConfiguration();
        }

		watchDogStatus = Result.RUNNING;
        Result result=Result.N_A;
        runIt=true;
		logger.info("Thread started.");
		logger.info("Running on " + javaRuntime.availableProcessors() + " CPU(s)"
			+ " max available memory for JVM " + javaRuntime.maxMemory() / 1024 + " KB");
		// renice - lower the priority
		
        printTracking=new PrintTracking(true);
       
       	mbean.graphStarted();

        //disabled by Kokon
//        Thread trackingThread=new Thread(printTracking, TRACKING_LOGGER_NAME);
//        trackingThread.setPriority(Thread.MIN_PRIORITY);
//        trackingThread.start();
        
       	Phase[] phases = graph.getPhases();
        for (currentPhaseNum = 0; currentPhaseNum < phases.length; currentPhaseNum++) {
            result=executePhase(phases[currentPhaseNum]);
            if(result == Result.ABORTED)      { 
                logger.error("!!! Phase execution aborted !!!");
                break;
            } else if(result == Result.ERROR) {
                logger.error("!!! Phase finished with error - stopping graph run !!!");
                break;
            }
            
            // force running of garbage collector
//            logger.info("Forcing garbage collection ...");
//            javaRuntime.runFinalization();
//            javaRuntime.gc();
        }
        mbean.graphFinished(result);
     
        if(finishJMX) {
        	finishJMX();
        }
        
        logger.info("WatchDog thread finished - total execution time: " + (System.currentTimeMillis() - startTimestamp) / 1000 + " (sec)");

        MDC.remove("runId");

		return result;
	}


    /**
     * 
     * @since 17.1.2007
     */
    private CloverJMX registerTrackingMBean(boolean register) {
       mbean = new CloverJMX(this,register);
        // register MBean
        // shall we really register our MBEAN ?
        if (!register) return mbean;
        
        String mbeanId = graph.getId();
        
        // Construct the ObjectName for the MBean we will register
        try {
            jmxObjectName = new ObjectName(
                    createMBeanName(mbeanId != null ? mbeanId : graph.getName(), this.getGraphRuntimeContext().getRunId())
            );
            // Register the  MBean
            mbs.registerMBean(mbean, jmxObjectName);

        } catch (MalformedObjectNameException ex) {
            logger.error(ex);
        } catch (InstanceAlreadyExistsException e) {
        	logger.error(e);
        } catch (MBeanRegistrationException e) {
        	logger.error(e);
        } catch (NotCompliantMBeanException e) {
        	logger.error(e);
        }
        return mbean;
    }

    /**
     * Creates identifier for shared JMX mbean.
     * @param defaultMBeanName
     * @return
     */
    public static String createMBeanName(String mbeanIdentifier) {
    	return createMBeanName(mbeanIdentifier, 0);
    }

    /**
     * Creates identifier for shared JMX mbean.
     * @param mbeanIdentifier
     * @param runId
     * @return
     */
    public static String createMBeanName(String mbeanIdentifier, long runId) {
        return "org.jetel.graph.runtime:type=" + MBEAN_NAME_PREFIX + (mbeanIdentifier != null ? mbeanIdentifier : "") + "_" + runId;
    }
    
//    public void runPhase(int phaseNo){
//        watchDogStatus = Result.RUNNING;
//        logger.info("Thread started.");
//        logger.info("Running on " + javaRuntime.availableProcessors() + " CPU(s)"
//            + " max available memory for JVM " + javaRuntime.freeMemory() / 1024 + " KB");
//        // renice - lower the priority
//        currentPhaseNum=-1;
//        
//        printTracking=new PrintTracking(true);
//        Thread trackingThread=new Thread(printTracking, TRACKING_LOGGER_NAME);
//        trackingThread.setPriority(Thread.MIN_PRIORITY);
//        trackingThread.start();
//        
//        for (int i = 0; i < phases.length; i++) {
//            if (phases[i].getPhaseNum()==phaseNo){
//                currentPhaseNum=i;
//                break;
//            }
//        }
//        if (currentPhaseNum>=0){
//            switch( executePhase(phases[currentPhaseNum]) ) {
//            case ABORTED:
//                watchDogStatus = Result.ABORTED;
//                logger.error("!!! Phase execution aborted !!!");
//                return;
//            case ERROR:
//                watchDogStatus = Result.ERROR;
//                logger.error("!!! Phase finished with error - stopping graph run !!!");
//                return;
//            }
//        }else{
//            watchDogStatus = Result.ERROR;
//            logger.error("!!! No such phase: "+phaseNo);
//            return;
//        }
//        
//        logger.info("Forcing garbage collection ...");
//        javaRuntime.runFinalization();
//        javaRuntime.gc();
//        
//        trackingThread.interrupt();
//        
//        watchDogStatus = Result.FINISHED_OK;
//        printPhasesSummary();
//    }

	/**
	 * Execute transformation - start-up all Nodes & watch them running
	 *
	 * @param  phase      Description of the Parameter
	 * @param  leafNodes  Description of the Parameter
	 * @return            Description of the Return Value
	 * @since             July 29, 2002
	 */
	public Result watch(Phase phase) throws InterruptedException {
		long phaseMemUtilizationMax;
		Iterator leafNodesIterator;
		Message message;
		int ticker = Defaults.WatchDog.NUMBER_OF_TICKS_BETWEEN_STATUS_CHECKS;
		long lastTimestamp;
		long currentTimestamp;
		long startTimestamp;
		long startTimeNano;
		Map<String, TrackingDetail> tracking = new LinkedHashMap<String, TrackingDetail>(
				phase.getNodes().size());
		List<Node> leafNodes;

		// let's create a copy of leaf nodes - we will watch them
		leafNodes = new LinkedList<Node>(phase.getNodes().values());
		// assign tracking info
		phase.setTracking(tracking);

		// get current memory utilization
		phaseMemUtilizationMax = memMXB.getHeapMemoryUsage().getUsed();
		// let's take timestamp so we can measure processing
		startTimestamp = lastTimestamp = System.currentTimeMillis();
		// also let's take nanotime to measure how much CPU we spend processing
		startTimeNano = System.nanoTime();

		printTracking.setTrackingInfo(tracking, phase.getPhaseNum());
		mbean.setRuningPhase(phase.getPhaseNum());
		mbean.setTrackingMap(tracking);
		mbean.updated();

		// entering the loop awaiting completion of work by all leaf nodes
		while (true) {
			// wait on error message queue
			message = inMsgQueue.poll(
					Defaults.WatchDog.WATCHDOG_SLEEP_INTERVAL,
					TimeUnit.MILLISECONDS);
			if (message != null) {
				switch(message.getType()){
				case ERROR:
					causeException = ((ErrorMsgBody) message.getBody())
							.getSourceException();
					causeGraphElement = message.getSender();
					logger.fatal("!!! Fatal Error !!! - graph execution is aborting");
					logger.error("Node "
							+ message.getSender().getId()
							+ " finished with status: "
							+ ((ErrorMsgBody) message.getBody())
									.getErrorMessage() + (causeException != null ? " caused by: " + causeException.getMessage() : ""));
					logger.debug("Node " + message.getSender().getId() + " error details:", causeException);
					abort();
					// printProcessingStatus(phase.getNodes().iterator(),
					// phase.getPhaseNum());
					printTracking.execute(true); // print tracking
					return Result.ERROR;
				case MESSAGE:
					synchronized (_MSG_LOCK) {
						outMsgMap.put(message.getRecipient(), message);
					}
					break;
				case NODE_FINISHED:
				default:
						// do nothing, just wake up
				}
			}

			// ------------------------------------
			// Check that we still have some nodes running
			// ------------------------------------

			if(message != null && message.getType() == Message.Type.NODE_FINISHED) {
				leafNodes.remove(message.getSender());
			}

			// is there any node running ?
			if (leafNodes.isEmpty()) {
				// gather tracking at nodes level
				phaseMemUtilizationMax = gatherNodeLevelTracking(phase,
						phaseMemUtilizationMax, startTimeNano, tracking);

				PhaseTrackingDetail phaseTracking = new PhaseTrackingDetail(
						phase.getPhaseNum(),
						(int) (System.currentTimeMillis() - startTimestamp),
						phaseMemUtilizationMax);
				phase.setPhaseTracking(phaseTracking);
				logger.info("Execution of phase [" + phase.getPhaseNum()
						+ "] successfully finished - elapsed time(sec): "
						+ phaseTracking.getExecTime() / 1000);
				// printProcessingStatus(phase.getNodes().iterator(),
				// phase.getPhaseNum());
				printTracking.execute(true);// print tracking
				return Result.FINISHED_OK;
				// nothing else to do in this phase
			}

			// -----------------------------------
			// from time to time perform some task
			// -----------------------------------
			if ((ticker--) == 0) {
				// reinitialize ticker
				ticker = Defaults.WatchDog.NUMBER_OF_TICKS_BETWEEN_STATUS_CHECKS;
				// gather tracking at nodes level
				phaseMemUtilizationMax = gatherNodeLevelTracking(phase,
						phaseMemUtilizationMax, startTimeNano, tracking);

				// Display processing status, if it is time
				currentTimestamp = System.currentTimeMillis();
				if (trackingInterval >= 0
						&& (currentTimestamp - lastTimestamp) >= trackingInterval) {
					// printProcessingStatus(phase.getNodes().iterator(),
					// phase.getPhaseNum());
					printTracking.execute(false); // print tracking
					lastTimestamp = currentTimestamp;

					// update mbean & signal that it was updated
					mbean.setRunningNodes(leafNodes.size());
					mbean.setRunTime(currentTimestamp - startTimestamp);
					mbean.updated();
				}
			}

			if (!runIt) {
				// we were forced to stop execution, stop all Nodes
				logger.error("User has interrupted graph execution !!!");
				abort();
				causeGraphElement = null;
				causeException = null;
				return Result.ABORTED;
			}
		}

	}


	private long gatherNodeLevelTracking(Phase phase, long phaseMemUtilizationMax, long startTimeNano, Map<String, TrackingDetail> tracking) {
		// get memory usage mark
		phaseMemUtilizationMax = Math.max(
		        phaseMemUtilizationMax, memMXB.getHeapMemoryUsage().getUsed() );
		
         
		long elapsedNano=System.nanoTime()-startTimeNano;
		// gather tracking information
		for (Node node : phase.getNodes().values()){
		    String nodeId=node.getId();
		    NodeTrackingDetail trackingDetail=(NodeTrackingDetail)tracking.get(nodeId);
		    int inPortsNum=node.getInPorts().size();
		    int outPortsNum=node.getOutPorts().size();
		    if (trackingDetail==null){
		        trackingDetail=new NodeTrackingDetail(nodeId,node.getName(),phase.getPhaseNum(),inPortsNum,outPortsNum);
		        tracking.put(nodeId, trackingDetail);
		    }
		    trackingDetail.timestamp();
		    trackingDetail.setResult(node.getResultCode());
		    //
		    long threadId=node.getNodeThread().getId();
		   // ThreadInfo tInfo=threadMXB.getThreadInfo(threadId);
		    if (threadCpuTimeIsSupported) {
		        trackingDetail.updateRunTime(threadMXB.getThreadCpuTime(threadId),
		                threadMXB.getThreadUserTime(threadId),
		                elapsedNano);
		    } else {
		        trackingDetail.updateRunTime(-1, -1, elapsedNano);
		    }
		    
		    int i=0;
		    for (InputPort port : node.getInPorts()){
		        trackingDetail.updateRows(PortType.IN_PORT, i, port.getInputRecordCounter());
		        trackingDetail.updateBytes(PortType.IN_PORT, i, port.getInputByteCounter());
		        i++;    
		    }
		    i=0;
		    for (OutputPort port : node.getOutPorts()){
		        trackingDetail.updateRows(PortType.OUT_PORT, i, port.getOutputRecordCounter());
		        trackingDetail.updateBytes(PortType.OUT_PORT, i, port.getOutputByteCounter());
		        
		       if (port instanceof Edge){
		            trackingDetail.updateWaitingRows(i, ((Edge)port).getBufferedRecords());
		       }
		       i++;
		    }
		    
		}
		return phaseMemUtilizationMax;
	}


	/**
	 *  Gets the Status of the WatchDog
	 *
	 * @return	Result of WatchDog run-time    
	 * @since     July 30, 2002
     * @see     org.jetel.graph.Result
	 */
	public Result getStatus() {
		return watchDogStatus;
	}

	
	/**  Outputs summary info about executed phases */
	void printPhasesSummary() {
		logger.info("-----------------------** Summary of Phases execution **---------------------");
		logger.info("Phase#            Finished Status         RunTime(sec)    MemoryAllocation(KB)");
		for (Phase phase : graph.getPhases()) {
			Object nodeInfo[] = {new Integer(phase.getPhaseNum()), phase.getResult().message(),
                    phase.getPhaseTracking() != null ? new Integer(phase.getPhaseTracking().getExecTimeSec()) : "",
                    phase.getPhaseTracking() != null ? new Integer(phase.getPhaseTracking().getMemUtilizationKB()) : ""};
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
	public void abort() {
        watchDogStatus=Result.ABORTED;

		// iterate through all the nodes and stop them
        for(Node node : currentPhase.getNodes().values()) {
			node.abort();
			logger.warn("Interrupted node: " + node.getId());
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodesIterator  Description of Parameter
	 * @param  leafNodesList  Description of Parameter
	 * @since                 July 31, 2002
	 */
	private void startUpNodes(Phase phase) {
		synchronized(threadManager) {
			while(threadManager.getFreeThreadsCount() < phase.getNodes().size()) { //it is sufficient, not necessary - so we have to time to time wake up and check it again
				try {
					threadManager.wait(); //from time to time thread is woken up to check the condition again
				} catch (InterruptedException e) {
					//DO NOTHING
				}
			}
			for(Node node: phase.getNodes().values()) {
				threadManager.executeNode(node);
				logger.debug(node.getId()+ " ... started");
			}
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  phase  Description of the Parameter
	 * @return        Description of the Return Value
	 */
	private Result executePhase(Phase phase) {
		currentPhase = phase;
		try {
			phase.init();
		} catch (ComponentNotReadyException e) {
			logger.error("Phase initialization failed with reason: " + e.getMessage(), e);
			return Result.ERROR;
		}
		logger.info("Starting up all nodes in phase [" + phase.getPhaseNum() + "]");
		startUpNodes(phase);
		logger.info("Sucessfully started all nodes in phase!");
		// watch running nodes in phase
        try{
            watchDogStatus = watch(phase);
        }catch(InterruptedException ex){
            watchDogStatus = Result.ABORTED;
        }
        
        //now we can notify all waiting phases for free threads
        synchronized(threadManager) {
        	threadManager.releaseNodeThreads(phase.getNodes().size());
            threadManager.notifyAll();
        }
        
        phase.setResult(watchDogStatus);
		return watchDogStatus;
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
    
	static class PrintTracking implements Runnable{
	    private static int[] ARG_SIZES_WITH_CPU = {-6,-4,28, -5, 9,12,7,8};
        private static int[] ARG_SIZES_WITHOUT_CPU = {38, -5, 9,12,7,8};
        Map<String,TrackingDetail> tracking;
        int phaseNo;
        Log trackingLogger;
        volatile boolean run;
        Thread thisThread;
        boolean displayTracking;
        
        PrintTracking(boolean displayTracking){
            trackingLogger= LogFactory.getLog(TRACKING_LOGGER_NAME);
            run=true;
            this.displayTracking=displayTracking;
        }
        
        void setTrackingInfo(Map<String,TrackingDetail> tracking, int phaseNo){
            this.tracking=tracking;
            this.phaseNo=phaseNo;
        }
        
        public void run() {
            thisThread=Thread.currentThread();
            while (run) {
                LockSupport.park();
                if (displayTracking) printProcessingStatus(false);
            }
        }
        
        public void execute(boolean finalTracking){
            LockSupport.unpark(thisThread);
            //added by Kokon
            if (displayTracking) printProcessingStatus(finalTracking);
            ////////////////
        }
        
        public void stop(){
            run=false;
            try{
                thisThread.join(100);
            }catch(InterruptedException ex){
                
            }
            if (thisThread.isAlive()){
                thisThread.interrupt();
            }
        }
        
        /**
         *  Outputs basic LOG information about graph processing
         *
         * @param  iterator  Description of Parameter
         * @param  phaseNo   Description of the Parameter
         * @since            July 30, 2002
         */
        private void printProcessingStatus(boolean finalTracking) {
            if (tracking==null) return;
            //StringBuilder strBuf=new StringBuilder(120);
            if (finalTracking)
                trackingLogger.info("----------------------** Final tracking Log for phase [" + phaseNo + "] **---------------------");
            else 
                trackingLogger.info("---------------------** Start of tracking Log for phase [" + phaseNo + "] **-------------------");
            // France is here just to get 24hour time format
            trackingLogger.info("Time: "
                + DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM, Locale.FRANCE).
                    format(Calendar.getInstance().getTime()));
            trackingLogger.info("Node                   Status     Port      #Records         #KB  Rec/s    KB/s");
            trackingLogger.info("----------------------------------------------------------------------------------");
            for (TrackingDetail nodeDetail : tracking.values()){
                Object nodeInfo[] = {nodeDetail.getNodeId(), nodeDetail.getResult().message()};
                int nodeSizes[] = {-23, -15};
                trackingLogger.info(StringUtils.formatString(nodeInfo, nodeSizes));
                //in ports
                Object portInfo[];
                boolean cpuPrinted=false;
                for(int i=0;i<nodeDetail.getNumInputPorts();i++){
                    if (i==0){
                        cpuPrinted=true;
                        final float cpuUsage= (finalTracking ? nodeDetail.getPeakUsageCPU()  : nodeDetail.getUsageCPU());
                        portInfo = new Object[] {" %cpu:",cpuUsage>=0.01f ? Float.toString(cpuUsage) : "..",
                                "In:", Integer.toString(i), 
                                Integer.toString(nodeDetail.getTotalRows(PortType.IN_PORT, i)),
                                Long.toString(nodeDetail.getTotalBytes(PortType.IN_PORT, i)>>10),
                                Integer.toString((nodeDetail.getAvgRows(PortType.IN_PORT, i))),
                                Integer.toString(nodeDetail.getAvgBytes(PortType.IN_PORT, i)>>10)};
                        trackingLogger.info(StringUtils.formatString(portInfo, ARG_SIZES_WITH_CPU)); 
                    }else{
                            portInfo = new Object[] {"In:", Integer.toString(i), 
                            Integer.toString(nodeDetail.getTotalRows(PortType.IN_PORT, i)),
                            Long.toString(nodeDetail.getTotalBytes(PortType.IN_PORT, i)>>10),
                            Integer.toString(( nodeDetail.getAvgRows(PortType.IN_PORT, i))),
                            Integer.toString(nodeDetail.getAvgBytes(PortType.IN_PORT, i)>>10)};
                        trackingLogger.info(StringUtils.formatString(portInfo, ARG_SIZES_WITHOUT_CPU));
                    }
                    
                }
                //out ports
                for(int i=0;i<nodeDetail.getNumOutputPorts();i++){
                    if (i==0 && !cpuPrinted){
                        final float cpuUsage= (finalTracking ? nodeDetail.getPeakUsageCPU()  : nodeDetail.getUsageCPU());
                        portInfo = new Object[] {" %cpu:",cpuUsage>0.01f ? Float.toString(cpuUsage) : "..",
                                "Out:", Integer.toString(i), 
                                Integer.toString(nodeDetail.getTotalRows(PortType.OUT_PORT, i)),
                                Long.toString(nodeDetail.getTotalBytes(PortType.OUT_PORT, i)>>10),
                                Integer.toString((nodeDetail.getAvgRows(PortType.OUT_PORT, i))),
                                Integer.toString(nodeDetail.getAvgBytes(PortType.OUT_PORT, i)>>10)};
                        trackingLogger.info(StringUtils.formatString(portInfo, ARG_SIZES_WITH_CPU));
                    }else{
                        portInfo = new Object[] {"Out:", Integer.toString(i), 
                            Integer.toString(nodeDetail.getTotalRows(PortType.OUT_PORT, i)),
                            Long.toString(nodeDetail.getTotalBytes(PortType.OUT_PORT, i)>>10),
                            Integer.toString((nodeDetail.getAvgRows(PortType.OUT_PORT, i))),
                            Integer.toString(nodeDetail.getAvgBytes(PortType.OUT_PORT, i)>>10)};
                        trackingLogger.info(StringUtils.formatString(portInfo, ARG_SIZES_WITHOUT_CPU));
                    }
                }               
            }
            trackingLogger.info("---------------------------------** End of Log **--------------------------------");
        }

    }
 
    
     public void sendMessage(Message msg) {
        inMsgQueue.add(msg);

    }

    public Message[] receiveMessage(GraphElement recipient, @SuppressWarnings("unused") final long wait) {
        Message[] msg = null;
        synchronized (_MSG_LOCK) {
            msg=(Message[])outMsgMap.getAll(recipient, new Message[0]);
            if (msg!=null) {
                outMsgMap.remove(recipient);
            }
        }
        return msg;
    }

    public boolean hasMessage(GraphElement recipient) {
        synchronized (_MSG_LOCK ){
            return outMsgMap.containsKey(recipient);
        }
    }

    public long getUsedMemory(Node node) {
        return -1;
    }

    public long getCpuTime(Node node) {
        return -1;
    }


    /**
     * Returns exception (reported by Node) which caused
     * graph to stop processing.<br>
     * 
     * @return the causeException
     * @since 7.1.2007
     */
    public Throwable getCauseException() {
        return causeException;
    }


    /**
     * Returns ID of Node which caused
     * graph to stop procesing.
     * 
     * @return the causeNodeID
     * @since 7.1.2007
     */
    public GraphElement getCauseGraphElement() {
        return causeGraphElement;
    }


    /**
     * Stop execution of graph
     * 
     * @since 29.1.2007
     */
    public void stopRun() {
        this.runIt = false;
    }


    /**
     * @return the graph
     * @since 26.2.2007
     */
    public TransformationGraph getTransformationGraph() {
        return graph;
    }


	public void setUseJMX(boolean useJMX) {
		this.provideJMX = useJMX;
	}

	public GraphRuntimeContext getGraphRuntimeContext() {
		return runtimeContext;
	}

	public CloverJMX getCloverJmx() {
		return mbean;
	}
	
	public boolean isFinishJMX() {
		return finishJMX;
	}

	public void setFinishJMX(boolean finishJMX) {
		this.finishJMX = finishJMX;
	}
	
}

