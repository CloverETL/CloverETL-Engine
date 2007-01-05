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
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.graph.Edge;
import org.jetel.graph.EdgeBase;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Phase;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.StringUtils;

import com.sun.tools.doclets.internal.toolkit.taglets.BaseExecutableMemberTaglet;

/**
 *  Description of the Class
 *
 * @author      dpavlis
 * @since       July 29, 2002
 * @revision    $Revision$
 */
public class WatchDog extends Thread implements CloverRuntime {
    
 public enum Status{
        
        READY(0,"READY"),
        RUNNING(1,"RUNNING"),
        ERROR(-1,"ERROR"),
        FINISHED_OK(2,"FINISHED_OK"),
        ABORTED(-2,"ABORTED");
        
        private final int code;
        private final String message;
        
        Status(int _code,String msg){
            code=_code;
            message=msg;
        }
        
        public int code(){return code;}
        public String message(){return message;}
        
    }
    
    
	private int trackingInterval;
	private Status watchDogStatus;
	private TransformationGraph graph;
	private Phase[] phases;
	private Phase currentPhase;
	private int currentPhaseNum;
	private Runtime javaRuntime;
	private int usedMemoryStampKB;
    private MemoryMXBean memMXB;
    private ThreadMXBean threadMXB;
    private BlockingQueue <Message> inMsgQueue;
    private BlockingQueue <Message> outMsgQueue;
    private Thread trackingThread;
    private PrintTracking printTracking;

    public final static String TRACKING_LOGGER_NAME = "Tracking";

    static Log logger = LogFactory.getLog(WatchDog.class);
    


	/**
	 *Constructor for the WatchDog object
	 *
	 * @param  graph   Description of the Parameter
	 * @param  phases  Description of the Parameter
	 * @since          September 02, 2003
	 */
	public WatchDog(TransformationGraph graph, Phase[] phases) {
		super("WatchDog");
		trackingInterval = Defaults.WatchDog.DEFAULT_WATCHDOG_TRACKING_INTERVAL;
		setDaemon(true);
		this.graph = graph;
		this.phases = phases;
		currentPhase = null;
		watchDogStatus = Status.READY;
		javaRuntime = Runtime.getRuntime();
        memMXB=ManagementFactory.getMemoryMXBean();
		usedMemoryStampKB = (int) memMXB.getHeapMemoryUsage().getUsed() /1024;
        threadMXB= ManagementFactory.getThreadMXBean();
        
        inMsgQueue=new PriorityBlockingQueue<Message>();
        outMsgQueue=new LinkedBlockingQueue<Message>();
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
	public WatchDog(TransformationGraph graph, Phase[] phases, int tracking) {
		this(graph,phases);
		trackingInterval = tracking;
	}


	/**  Main processing method for the WatchDog object */
	public void run() {
		watchDogStatus = Status.RUNNING;
		logger.info("Thread started.");
		logger.info("Running on " + javaRuntime.availableProcessors() + " CPU(s)"
			+ " max available memory for JVM " + javaRuntime.freeMemory() / 1024 + " KB");
		// renice - lower the priority
		setPriority(Thread.MIN_PRIORITY);
		
        printTracking=new PrintTracking();
        trackingThread=new Thread(printTracking, TRACKING_LOGGER_NAME);
        trackingThread.setPriority(Thread.MIN_PRIORITY);
        trackingThread.start();
        
		for (currentPhaseNum = 0; currentPhaseNum < phases.length; currentPhaseNum++) {
			if (!executePhase(phases[currentPhaseNum])) {
				watchDogStatus = Status.ERROR;
				logger.error("!!! Phase finished with error - stopping graph run !!!");
				return;
			}
			// force running of garbage collector
			logger.info("Forcing garbage collection ...");
			javaRuntime.runFinalization();
			javaRuntime.gc();
		}

        trackingThread.interrupt();
		watchDogStatus = Status.FINISHED_OK;
		printPhasesSummary();
	}

    public void runPhase(int phaseNo){
        watchDogStatus = Status.RUNNING;
        logger.info("Thread started.");
        logger.info("Running on " + javaRuntime.availableProcessors() + " CPU(s)"
            + " max available memory for JVM " + javaRuntime.freeMemory() / 1024 + " KB");
        // renice - lower the priority
        setPriority(Thread.MIN_PRIORITY);
        currentPhaseNum=-1;
        
        printTracking=new PrintTracking();
        trackingThread=new Thread(printTracking, TRACKING_LOGGER_NAME);
        trackingThread.setPriority(Thread.MIN_PRIORITY);
        trackingThread.start();
        
        for (int i = 0; i < phases.length; i++) {
            if (phases[i].getPhaseNum()==phaseNo){
                currentPhaseNum=i;
                break;
            }
        }
        if (currentPhaseNum>=0){
            if (!executePhase(phases[currentPhaseNum])) {
                watchDogStatus = Status.ERROR;
                logger.error("!!! Phase finished with error - stopping graph run !!!");
                return;
            }
        }else{
            watchDogStatus = Status.ERROR;
            logger.error("!!! No such phase: "+phaseNo);
            return;
        }
        
        logger.info("Forcing garbage collection ...");
        javaRuntime.runFinalization();
        javaRuntime.gc();
        
        trackingThread.interrupt();
        
        watchDogStatus = Status.FINISHED_OK;
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
	public Status watch(Phase phase, List leafNodes) throws InterruptedException {
		int phaseMemUtilizationMaxKB;
		Iterator leafNodesIterator;
        Message message;
		int ticker = Defaults.WatchDog.NUMBER_OF_TICKS_BETWEEN_STATUS_CHECKS;
		long lastTimestamp;
		long currentTimestamp;
		long startTimestamp;
        long startTimeNano;
        Map<String,TrackingDetail> tracking=new LinkedHashMap<String,TrackingDetail>(phase.getNodes().size());
        
		//get current memory utilization
		phaseMemUtilizationMaxKB=(int)memMXB.getHeapMemoryUsage().getUsed()/1024;
		//let's take timestamp so we can measure processing
		startTimestamp = lastTimestamp = System.currentTimeMillis();
        // also let's take nanotime to measure how much CPU we spend processing
        startTimeNano=System.nanoTime();
        
        printTracking.setTrackingInfo(tracking, phase.getPhaseNum());
	
        // entering the loop awaiting completion of work by all leaf nodes
		while (true) {
            // wait on error message queue
            message=inMsgQueue.poll(Defaults.WatchDog.WATCHDOG_SLEEP_INTERVAL, TimeUnit.MILLISECONDS);
            if (message != null) {
                if (message.getType() == Message.Type.ERROR) {
                    logger
                            .fatal("!!! Fatal Error !!! - graph execution is aborting");
                    logger.error("Node " + message.getSenderID()
                            + " finished with fatal error: "
                            + message.getBody());
                    abort();
                    // printProcessingStatus(phase.getNodes().iterator(),
                    // phase.getPhaseNum());
                    printTracking.execute(); // print tracking
                    return Status.ERROR;
                } else {
                    //TODO: sending of messages (non errors)
                }
            }
            
			if (leafNodes.isEmpty()) {
				phase.setPhaseMemUtilization(phaseMemUtilizationMaxKB);
				phase.setPhaseExecTime((int) (System.currentTimeMillis() - startTimestamp));
				logger.info("Execution of phase [" + phase.getPhaseNum() + "] successfully finished - elapsed time(sec): "
						+ phase.getPhaseExecTime() / 1000);
				//printProcessingStatus(phase.getNodes().iterator(), phase.getPhaseNum());
                printTracking.execute();// print tracking
				return Status.FINISHED_OK;
				// nothing else to do in this phase
			}
            
            
            // ------------------------------------
            // Check that we still have some nodes running
            // ------------------------------------
            
			leafNodesIterator = leafNodes.iterator();
			while (leafNodesIterator.hasNext()) {
				Node node = (Node) leafNodesIterator.next();
				// is this Node still alive - ? doing something
				if (!node.getNodeThread().isAlive()) {
					leafNodesIterator.remove();
				}
			}
            //  -----------------------------------
			//  from time to time perform some task
            //  -----------------------------------
            if ((ticker--) == 0) {
                // reinitialize ticker
                ticker = Defaults.WatchDog.NUMBER_OF_TICKS_BETWEEN_STATUS_CHECKS;
                // get memory usage mark
                phaseMemUtilizationMaxKB = (int) Math.max(
                        phaseMemUtilizationMaxKB, memMXB.getHeapMemoryUsage()
                                .getUsed() / 1024);
                }
            
                long elapsedNano=System.nanoTime()-startTimeNano;
                // gather tracking information
                for (Node node : phase.getNodes()){
                    String nodeId=node.getId();
                    TrackingDetail trackingDetail=tracking.get(nodeId);
                    int inPortsNum=node.getInPorts().size();
                    int outPortsNum=node.getOutPorts().size();
                    if (trackingDetail==null){
                        trackingDetail=new TrackingDetail(nodeId,inPortsNum,outPortsNum);
                        tracking.put(nodeId, trackingDetail);
                    }
                    trackingDetail.timestamp();
                    trackingDetail.setResult(node.getResultCode());
                    //
                    long threadId=node.getNodeThread().getId();
                   // ThreadInfo tInfo=threadMXB.getThreadInfo(threadId);
                    trackingDetail.updateRunTime(threadMXB.getThreadCpuTime(threadId),
                                    threadMXB.getThreadUserTime(threadId),
                                    elapsedNano);
                    
                    int i=0;
                    for (InputPort port : node.getInPorts()){
                        trackingDetail.updateRows(TrackingDetail.IN_PORT, i, port.getRecordCounter());
                        trackingDetail.updateBytes(TrackingDetail.IN_PORT, i, port.getByteCounter());
                        i++;    
                    }
                    i=0;
                    for (OutputPort port : node.getOutPorts()){
                        trackingDetail.updateRows(TrackingDetail.OUT_PORT, i, port.getRecordCounter());
                        trackingDetail.updateBytes(TrackingDetail.OUT_PORT, i, port.getByteCounter());
                        
                       if (port instanceof Edge){
                            trackingDetail.updateWaitingRows(i, ((Edge)port).getBufferedRecords());
                       }
                       i++;
                    }
                    
                }
                
                // Display processing status, if it is time
                currentTimestamp = System.currentTimeMillis();
                if ((currentTimestamp - lastTimestamp) >= trackingInterval) {
                    //printProcessingStatus(phase.getNodes().iterator(), phase.getPhaseNum());
                    printTracking.execute(); // print tracking
                    lastTimestamp = currentTimestamp;
                }
            }
			
	}


	/**
	 *  Gets the Status of the WatchDog
	 *
	 * @return	0 READY, -1 ERROR, 1 RUNNING, 2 FINISHED OK    
	 * @since     July 30, 2002
	 */
	public Status getStatus() {
		return watchDogStatus;
	}

	
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
	public void abort() {
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
	private void startUpNodes(Iterator nodesIterator, List<Node> leafNodesList) {
		Node node;
		while (nodesIterator.hasNext()) {
			node = (Node) nodesIterator.next();
            Thread nodeThread = new Thread(node, node.getId());
          // this thread is daemon - won't live if main thread ends
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
	private boolean executePhase(Phase phase) {
		currentPhase = phase;
		List<Node> leafNodes = new LinkedList<Node>();
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
        try{
            watchDogStatus = watch(phase, leafNodes);
        }catch(InterruptedException ex){
            watchDogStatus = Status.ERROR;
        }
        
        // following code is not needed any more
//        // check how nodes in phase finished
//        Node node;
//        for (Iterator i=phase.getNodes().iterator();i.hasNext();){
//            node=(Node)i.next();
//            // was there an uncaught error ?
//            if (node.getResultCode()==Node.Result.ERROR){
//               result=false;
//               logger.error("in node "+node.getId()+" : "+node.getResultMsg());
//            }
//        }
		
        //end of phase, destroy it
		phase.free();
        
		return (watchDogStatus==Status.FINISHED_OK) ? true : false;
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
	    Map<String,TrackingDetail> tracking;
        int phaseNo;
        Log trackingLogger;
        volatile boolean run;
        
        PrintTracking(){
            trackingLogger= LogFactory.getLog(TRACKING_LOGGER_NAME);
            run=true;
        }
        
        void setTrackingInfo(Map<String,TrackingDetail> tracking, int phaseNo){
            this.tracking=tracking;
            this.phaseNo=phaseNo;
        }
        
        public void run() {
            while (run) {
                LockSupport.park();
                printProcessingStatus();
            }
        }
        
        public void execute(){
            LockSupport.unpark(trackingThread);
        }
        
        public void stop(){
            run=false;
        }
        
        /**
         *  Outputs basic LOG information about graph processing
         *
         * @param  iterator  Description of Parameter
         * @param  phaseNo   Description of the Parameter
         * @since            July 30, 2002
         */
        private void printProcessingStatus() {
            if (tracking==null) return;
            //StringBuilder strBuf=new StringBuilder(120);
            trackingLogger.info("---------------------** Start of tracking Log for phase [" + phaseNo + "] **-------------------");
            // France is here just to get 24hour time format
            trackingLogger.info("Time: "
                + DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM, Locale.FRANCE).
                    format(Calendar.getInstance().getTime()));
            trackingLogger.info("Node                   Status     Port      #Records         #KB  Rec/s   KB/s");
            trackingLogger.info("----------------------------------------------------------------------------------");
            for (TrackingDetail nodeDetail : tracking.values()){
                Object nodeInfo[] = {nodeDetail.getNodeId(), nodeDetail.getResult().message()};
                int nodeSizes[] = {-23, -10};
                trackingLogger.info(StringUtils.formatString(nodeInfo, nodeSizes));
                //in ports
                Object portInfo[];
                int portSizes[];
                boolean cpuPrinted=false;
                for(int i=0;i<nodeDetail.getNumInputPorts();i++){
                    if (i==0){
                        cpuPrinted=true;
                        portInfo = new Object[] {"%CPU:",Float.toString(nodeDetail.getUsageCPU()),
                                "In:", Integer.toString(i), 
                                Integer.toString(nodeDetail.getTotalRows(TrackingDetail.IN_PORT, i)),
                                Long.toString(nodeDetail.getTotalBytes(TrackingDetail.IN_PORT, i)>>10),
                                Integer.toString(nodeDetail.getAvgRows(TrackingDetail.IN_PORT, i)),
                                Integer.toString(nodeDetail.getAvgBytes(TrackingDetail.IN_PORT, i)>>10)};
                        portSizes = new int[] {-5,-4,29, -5, 9,12,7,8};
                    }else{
                            portInfo = new Object[] {"In:", Integer.toString(i), 
                            Integer.toString(nodeDetail.getTotalRows(TrackingDetail.IN_PORT, i)),
                            Long.toString(nodeDetail.getTotalBytes(TrackingDetail.IN_PORT, i)>>10),
                            Integer.toString(nodeDetail.getAvgRows(TrackingDetail.IN_PORT, i)),
                            Integer.toString(nodeDetail.getAvgBytes(TrackingDetail.IN_PORT, i)>>10)};
                            portSizes = new int[] {38, -5, 9,12,7,8};
                    }
                    trackingLogger.info(StringUtils.formatString(portInfo, portSizes));
                }
                //out ports
                for(int i=0;i<nodeDetail.getNumOutputPorts();i++){
                    if (i==0 && !cpuPrinted){
                        portInfo = new Object[] {"%CPU:",Float.toString(nodeDetail.getUsageCPU()),
                                "Out:", Integer.toString(i), 
                                Integer.toString(nodeDetail.getTotalRows(TrackingDetail.OUT_PORT, i)),
                                Long.toString(nodeDetail.getTotalBytes(TrackingDetail.OUT_PORT, i)>>10),
                                Integer.toString(nodeDetail.getAvgRows(TrackingDetail.OUT_PORT, i)),
                                Integer.toString(nodeDetail.getAvgBytes(TrackingDetail.OUT_PORT, i)>>10),
                                Integer.toString(nodeDetail.getAvgWaitingRows(i))};
                        portSizes = new int[] {-5,-4,29, -5, 9,12,7,8,4};
                    }else{
                        portInfo = new Object[] {"Out:", Integer.toString(i), 
                            Integer.toString(nodeDetail.getTotalRows(TrackingDetail.OUT_PORT, i)),
                            Long.toString(nodeDetail.getTotalBytes(TrackingDetail.OUT_PORT, i)>>10),
                            Integer.toString(nodeDetail.getAvgRows(TrackingDetail.OUT_PORT, i)),
                            Integer.toString(nodeDetail.getAvgBytes(TrackingDetail.OUT_PORT, i)>>10),
                            Integer.toString(nodeDetail.getAvgWaitingRows(i))};
                        portSizes = new int[] {38, -5, 9,12,7,8,4};
                    }
                    trackingLogger.info(StringUtils.formatString(portInfo, portSizes));
                }
                /*
                strBuf.setLength(0);
                strBuf.append("Run status - cpu time:").append(nodeDetail.getTotalCPUTime());
                strBuf.append(" user time:").append(nodeDetail.getTotalUserTime());
                strBuf.append(" %CPU:").append(nodeDetail.getUsageCPU());
                strBuf.append(" %USER:").append(nodeDetail.getUsageUser());
                trackingLogger.info(strBuf);*/
            }
            trackingLogger.info("---------------------------------** End of Log **--------------------------------");
        }

    }
 
    
     public void sendMessage(Message msg) {
        inMsgQueue.add(msg);

    }

    public Message receiveMessage(String recipientNodeID,long wait) {
        Message msg;
        do{
        for (Iterator<Message> it = outMsgQueue.iterator(); it.hasNext();) {
            msg = it.next();
            if (msg.getRecipientID().equals(recipientNodeID)) {
                it.remove();
                return msg;
            }
        }
        }while(wait>0);
        return null;
    }

    public boolean hasMessage() {
        return !outMsgQueue.isEmpty();
    }

    public long getUsedMemory(Node node) {
        return -1;
    }

    public long getCpuTime(Node node) {
        return -1;
    }
    
    
}

