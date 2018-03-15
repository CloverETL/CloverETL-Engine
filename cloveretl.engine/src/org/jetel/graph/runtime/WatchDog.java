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
package org.jetel.graph.runtime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.WriterAppender;
import org.jetel.enums.EnabledEnum;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.CompoundException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.ContextProvider.Context;
import org.jetel.graph.GraphElement;
import org.jetel.graph.IGraphElement;
import org.jetel.graph.Node;
import org.jetel.graph.Phase;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.dictionary.DictionaryValuesContainer;
import org.jetel.graph.runtime.jmx.CloverJMX;
import org.jetel.graph.runtime.jmx.CloverJMXMBean;
import org.jetel.graph.runtime.jmx.GraphTrackingDetail;
import org.jetel.graph.runtime.tracker.TokenTracker;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.LogUtils;
import org.jetel.util.primitive.MultiValueMap;
import org.jetel.util.property.PropertyRefResolver;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


/**
 *  Description of the Class
 *
 * @author      dpavlis
 * @since       July 29, 2002
 */
public class WatchDog implements Callable<Result>, CloverPost {

	/**
	 * This lock object guards currentPhase variable and watchDogStatus. 
	 */
	private final Lock currentPhaseLock = new ReentrantLock();

	private final Object abortMonitor = new Object();
	private boolean abortFinished = false;
	
    public final static long WAITTIME_FOR_STOP_SIGNAL = 5000; //miliseconds

	private static final long ABORT_TIMEOUT = 5000L;
	private static final long ABORT_WAIT = 2400L;

	public static final String WATCHDOG_THREAD_NAME_PREFIX = "WatchDog_";
	
    private final Object messageMonitor = new Object();
    
    private static Logger logger = Logger.getLogger(WatchDog.class);

	/**
     * Thread manager is used to run nodes as threads.
     */
    private IThreadManager threadManager;
	private volatile Result watchDogStatus;
	private TransformationGraph graph;
	private Phase currentPhase;
    private BlockingQueue <Message<?>> inMsgQueue;
    private MultiValueMap<IGraphElement, Message<?>> outMsgMap;
    private volatile Throwable causeException;
    private volatile IGraphElement causeGraphElement;
//    private volatile boolean runIt;
    private boolean provideJMX = true;
    private boolean finishJMX = true; //whether the JMX mbean should be unregistered on the graph finish 
    private final GraphRuntimeContext runtimeContext;
    
    private TokenTracker tokenTracker;

    /**
     * This flag indicates that the {@link #free()} method has been invoked.
     * The flag is used for correct abortion of WatchDog thread if necessary. 
     */
    private volatile boolean isReleased = false;
    
    
	/**
	 * Tracking information about the running graph. The tracking information
	 * are available for clients using {@link CloverJMX} mBean.
	 */
	private GraphTrackingDetail graphTracking;

    /**
     * Synchronized indication of phase number, which can be executed.
     * This mechanism is used in clustered graphs, where all graphs (partitions)
     * must be synchronized on phases.
     */
    private volatile int approvedPhaseNumber = Integer.MIN_VALUE;

    /**
     * Log4j file appender, which is created on each graph execution for graph specific logging.
     * This is just cache for later release.
     * @see com.cloveretl.server.worker.runtime.ExecutionHelper.releaseWatchdog(WatchDog watchDog)
     */
    private WriterAppender graphLogAppender;
    
	/**
	 *Constructor for the WatchDog object
	 *
	 * @param  graph   Description of the Parameter
	 * @param  phases  Description of the Parameter
	 * @since          September 02, 2003
	 */
	public WatchDog(TransformationGraph graph, GraphRuntimeContext runtimeContext) {
		graph.setWatchDog(this);
		this.graph = graph;
		this.runtimeContext = runtimeContext;
		currentPhase = null;
		watchDogStatus = Result.N_A;
        
		inMsgQueue = new LinkedBlockingQueue<Message<?>>();
		outMsgMap = new MultiValueMap<IGraphElement, Message<?>>(Collections.synchronizedMap(new HashMap<IGraphElement, List<Message<?>>>()));
        
        //is JMX turned on?
        provideJMX = runtimeContext.useJMX();
        
        //passes a password from context to the running graph
        graph.setPassword(runtimeContext.getPassword());
	}

	/**
	 * WatchDog initialization.
	 */
	public void init() {
		//at least simple thread manager will be used
		if(threadManager == null) {
			threadManager = new SimpleThreadManager();
		}

		//create token tracker if graph is jobflow type
		if (graph.getRuntimeJobType().isJobflow()) {
			tokenTracker = new TokenTracker(graph);
		}
		
		//initialize graph tracking
		graphTracking = new GraphTrackingDetail(graph);
		
		//start CloverJMX
		if (provideJMX) {
			CloverJMX.getInstance().registerWatchDog(this);
		}

       	//watchdog is now ready to use
		watchDogStatus = Result.READY;
	}
	
	/**  Main processing method for the WatchDog object */
	@Override
	public Result call() {
		if (logger.isDebugEnabled()) {
		  	logger.debug("Watchdog thread is running");
		}
		currentPhaseLock.lock();
		String originalThreadName = null;
		final long startTimestamp = System.nanoTime();

		//we have to register current watchdog's thread to context provider - from now all 
		//ContextProvider.getGraph() invocations return proper transformation graph
		Context c = ContextProvider.registerGraph(graph);
		try {
			try {
				if (watchDogStatus == Result.ABORTED) { //graph has been aborted before real execution
					return watchDogStatus;
				}
				
				//thread context classloader is preset to a reasonable classloader
				//this is just for sure, threads are recycled and no body can guarantee which context classloader remains preset
				Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
	
	    		MDC.put(LogUtils.MDC_RUNID_KEY, runtimeContext.getRunId());
	    		
	    		Thread t = Thread.currentThread();
	    		originalThreadName = t.getName();
				String newThreadName = WATCHDOG_THREAD_NAME_PREFIX + runtimeContext.getRunId();
				if (logger.isTraceEnabled()) {
					logger.trace("rename thread " + originalThreadName + " to " + newThreadName);
				}
			  	t.setName(newThreadName);
	    		
			  	logger.debug("Authority proxy: " + getAuthorityProxy().getClass().getName());
			  	
			  	logger.info("Job execution type: " + getGraphRuntimeContext().getJobType());
			  	
	    		//print graph properties
	    		graph.getGraphParameters().printContent(logger, "Job parameters: \n"); 
	    		
	    		//print runtime classpath
	    		logger.info("Runtime classpath: " + Arrays.toString(graph.getRuntimeContext().getRuntimeClassPath()));
	    		
	    		//print out runtime context
	    		logger.debug("Graph runtime context: " + graph.getRuntimeContext().getAllProperties());
	    		
	    		if (graph.getRuntimeJobType().isSubJob()) {
	    			logger.info("Connected input ports: " + graph.getRuntimeContext().getParentGraphInputPortsConnected());
	    			logger.info("Connected output ports: " + graph.getRuntimeContext().getParentGraphOutputPortsConnected());
	    		}
	    		
	    		//print information about conditionally enabled and all disabled components
	    		printComponentsEnabledStatus();
	    		
	    		//print initial dictionary content
	    		graph.getDictionary().printContent(logger, "Initial dictionary content:");

	    		if (runtimeContext.isVerboseMode()) {
	                // this can be called only after graph.init()
	                graph.dumpGraphConfiguration();
	            }
	
	    		watchDogStatus = Result.RUNNING;
	
	    		//creates tracking logger for cloverJMX mbean
	    		if (provideJMX) {
	    			TrackingLogger.track(this);
	    		}
	          	
	           	graphStarted();
	
	           	//pre-execute initialization of graph
	           	try {
	           		graph.preExecute();
	           	} catch (Exception e) {
	    			setCauseException(e);
	    			if (e instanceof ComponentNotReadyException) {
	    				setCauseGraphElement(((ComponentNotReadyException) e).getGraphElement());
	    			}
	           		watchDogStatus = Result.ERROR;
	           		ExceptionUtils.logException(logger, "Graph pre-execute initialization failed.", e);
	           	}
	
	           	//run all phases
	           	if (watchDogStatus == Result.RUNNING) {
		           	Phase[] phases = graph.getPhases();
		           	Result phaseResult = Result.N_A;
		           	if (phases.length > 0) { //non-empty graph
			           	for (int currentPhaseNum = 0; currentPhaseNum < phases.length; currentPhaseNum++) {
			           		//if the graph runs in synchronized mode we need to wait for synchronization event to process next phase
			           		if (runtimeContext.isSynchronizedRun()) {
			           			logger.info("Waiting for phase " + phases[currentPhaseNum] + " approval...");
		           				watchDogStatus = Result.WAITING;
		           				currentPhaseLock.unlock();
			           			synchronized (CloverJMX.getInstance()) {
				           			while (approvedPhaseNumber < phases[currentPhaseNum].getPhaseNum() 
				           					&& watchDogStatus == Result.WAITING) { //graph was maybe aborted
				           				try {
				           					CloverJMX.getInstance().wait();
				           				} catch (InterruptedException e) {
				           					throw new RuntimeException("WatchDog was interrupted while was waiting for phase synchronization event.");
				           				}
				           			}
			           			}
		           				currentPhaseLock.lock();
		           				//watchdog was aborted while was waiting for next phase approval
		           				if (watchDogStatus == Result.ABORTED) {
		    	                    logger.warn("Job execution aborted");
		    	                    break;
		           				} else {
		           					watchDogStatus = Result.RUNNING;
		           				}
			           		}
			           		phaseStarted(phases[currentPhaseNum]);
			           		//execute phase
			                phaseResult = executePhase(phases[currentPhaseNum]);
			                phases[currentPhaseNum].setResult(phaseResult);
			                
			                if(phaseResult == Result.ABORTED) {
			                	phaseAborted();
			                    logger.warn("Phase execution aborted");
			                    break;
			                } else if (phaseResult == Result.ERROR) {
			                	phaseError(getErrorMessage());
			                    logger.error("Phase finished with error - stopping job run");
			                    break;
			                }
			           		phaseFinished();
			            }
		           	} else {
		           		//empty graph execution is successful 
		           		logger.info("Transformation with no components has been executed.");
		           		watchDogStatus = Result.FINISHED_OK;
		           	}
		           	//post-execution of graph
		           	try {
		           		graph.postExecute();
		           	} catch (Exception e) {
		    			setCauseException(e);
		    			if (e instanceof ComponentNotReadyException) {
		    				setCauseGraphElement(((ComponentNotReadyException) e).getGraphElement());
		    			}
		           		watchDogStatus = Result.ERROR;
		           		ExceptionUtils.logException(logger, "Job post-execute method failed.", e);
		           	}
	
		           	//aborted graph does not follow last phase status
		           	if (watchDogStatus == Result.RUNNING) {
		           		watchDogStatus = phaseResult;
		           	}
	           	}
	
	           	//commit or rollback
	           	if (watchDogStatus == Result.FINISHED_OK) {
	           		try {
	           			graph.commit();
	           		} catch (Exception e) {
	           			setCauseException(e);
		           		watchDogStatus = Result.ERROR;
		           		ExceptionUtils.logException(logger, "Job commit failed", e, Level.FATAL);
	           		}
	           	} else {
	           		try {
	           			graph.rollback();
	           		} catch (Exception e) {
	           			setCauseException(e);
		           		watchDogStatus = Result.ERROR;
		           		ExceptionUtils.logException(logger, "Job rollback failed", e, Level.FATAL);
	           		}
	           	}

	           	//print initial dictionary content
	    		graph.getDictionary().printContent(logger, "Final dictionary content:");
	       	} catch (Throwable t) {
	       		//something was seriously wrong, let's abort the graph if necessary and report the error
				currentPhaseLock.unlock(); //current phase monitor needs to be unlocked before aborting
				try {
					abort(false);
				} catch (Exception e) {
		       		setCauseException(t);
				}
	       		
	       		setCauseException(t);
	       		setCauseGraphElement(null);
	       		watchDogStatus = Result.ERROR;
	       		ExceptionUtils.logException(logger, "Error watchdog execution", t);
	       	} finally {
	           	sendFinalJmxNotification();
	            logger.info("WatchDog thread finished - total execution time: " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTimestamp) + " (sec)");
			}
		} finally {
            //we have to unregister current watchdog's thread from context provider
			ContextProvider.unregister(c);

			currentPhaseLock.unlock();
			
			if (originalThreadName != null) {
				Thread.currentThread().setName(originalThreadName);
			}
            MDC.remove(LogUtils.MDC_RUNID_KEY);
		}

		return watchDogStatus;
	}

	private void sendFinalJmxNotification() {
		sendFinalJmxNotification0();
		
		//if the graph was aborted, now the aborting thread is waiting for final notification - this is the way how to send him notice about the graph finished right now
		synchronized (abortMonitor) {
			abortFinished = true;
			abortMonitor.notifyAll();
		}
	}

	private void sendFinalJmxNotification0() {
		switch (watchDogStatus) {
		case FINISHED_OK:
			graphFinished();
			break;
		case ABORTED:
			graphAborted();
			break;
		case ERROR:
			graphError(getErrorMessage());
			break;
		default:
			break;
		}
	}
	
	/**
	 * Execute transformation - start-up all Nodes & watch them running
	 *
	 * @param  phase      Description of the Parameter
	 * @param  leafNodes  Description of the Parameter
	 * @return            Description of the Return Value
	 * @since             July 29, 2002
	 */
    @SuppressFBWarnings("UL")
	private Result watch(Phase phase) throws InterruptedException {
		Message<?> message;

		// let's create a copy of leaf nodes - we will watch them
		Set<Node> phaseNodes = new HashSet<Node>(phase.getNodes().values());

		// is there any node running ? - this test is necessary for phases without nodes - empty phase
		if (phaseNodes.isEmpty()) {
			return watchDogStatus != Result.ABORTED ? Result.FINISHED_OK : Result.ABORTED;
		}

		// entering the loop awaiting completion of work by all leaf nodes
		while (true) {
			// wait on error message queue
			currentPhaseLock.unlock();
			try {
				message = inMsgQueue.poll(runtimeContext.getTrackingInterval(), TimeUnit.MILLISECONDS);
			} finally {
				currentPhaseLock.lock();
			}
			
			if (message != null) {
				switch (message.getType()){
				case ERROR:
					setCauseException(((ErrorMsgBody) message.getBody()).getSourceException());
					setCauseGraphElement(message.getSender());
					
					if (getCauseException() == null) {
						setCauseException(new JetelRuntimeException(String.format("Graph element %s failed with unknown cause.", message.getSender())));
					}
					ExceptionUtils.logException(logger, null, getCauseException());
					return Result.ERROR;
				case MESSAGE:
					synchronized (messageMonitor) {
						if (message.getRecipient() != null) {
							outMsgMap.putValue(message.getRecipient(), message);
						}
					}
					break;
				case NODE_FINISHED:
					phaseNodes.remove(message.getSender());
					nodeFinished(message.getSender().getId());
					break;
				default:
					// do nothing, just wake up
				}
			}


			// is there any node running ?
			if (phaseNodes.isEmpty()) {
				return watchDogStatus != Result.ABORTED ? Result.FINISHED_OK : Result.ABORTED;
			}

			if (isReleased) {
				//WatchDog#free() method has been invoked, so the running graph has been released (free method) as well
				//so no more messages will come, let's finish this phase watching
				//this can happen if abortion of a component failed
				return Result.ABORTED;
			}
			
			// gather graph tracking
			//ETL graphs are tracked only in regular intervals, jobflows are tracked more precise, whenever something happens
			if (message == null || ContextProvider.getRuntimeJobType().isJobflow()) {
				gatherTrackingDetails();
			}
		}
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

	/**
	 * aborts execution of current phase
	 *
	 * @since    July 29, 2002
	 */
	public void abort() {
		abort(true);
	}

    @SuppressFBWarnings("NN_NAKED_NOTIFY")
	public void abort(boolean waitForAbort) {
		final Object oldMDCRunId = MDC.get(LogUtils.MDC_RUNID_KEY);
		try {
			//update MDC for current thread to route logging message to correct logging destination 
			MDC.put(LogUtils.MDC_RUNID_KEY, runtimeContext.getRunId());

			currentPhaseLock.lock();
			if (watchDogStatus == Result.N_A || watchDogStatus == Result.READY) {
				waitForAbort = false;
			}
			//only running or waiting graph can be aborted
			if (watchDogStatus != Result.RUNNING && watchDogStatus != Result.WAITING) {
				//if the graph status is not final, so the graph was aborted
				if (!watchDogStatus.isStop()) {
			        watchDogStatus = Result.ABORTED;
				}
				return;
			}

			//if the phase is running broadcast all nodes in the phase they should be aborted
			if (watchDogStatus == Result.RUNNING) { 
		        watchDogStatus = Result.ABORTED;
				// iterate through all the nodes and stop them
		        for (Node node : currentPhase.getNodes().values()) {
					node.abort();
					logger.warn("Interrupted node: " + node.getId());
				}
			}
			//if the graph is waiting on a phase synchronization point the watchdog is woken up with current status ABORTED 
			if (watchDogStatus == Result.WAITING) {
		        watchDogStatus = Result.ABORTED;
				synchronized (CloverJMX.getInstance()) {
					CloverJMX.getInstance().notifyAll();
				}
			}
		} catch (RuntimeException e) {
			throw new JetelRuntimeException("Graph abort failed.", e);
		} finally {
			try {
				synchronized (abortMonitor) {
					currentPhaseLock.unlock();
					if (waitForAbort) {
						long startAbort = System.currentTimeMillis();
						while (!abortFinished) {
							long interval = System.currentTimeMillis() - startAbort;
							if (interval > ABORT_TIMEOUT) {
								throw new IllegalStateException("Graph aborting error! Timeout " + ABORT_TIMEOUT + "ms exceeded!");
							}
					        try {
					        	//the aborting thread try to wait for end of graph run
								abortMonitor.wait(ABORT_WAIT);
							} catch (InterruptedException ignore) {	}// catch
						}// while
					}
				}// synchronized
			} finally {
				//rollback MDC
				MDC.remove(LogUtils.MDC_RUNID_KEY);
				if (oldMDCRunId != null) {
					MDC.put(LogUtils.MDC_RUNID_KEY, oldMDCRunId);
				}
			}
		}// finally
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
			while (threadManager.getFreeThreadsCount() < phase.getNodes().size()) { //it is sufficient, not necessary condition - so we have to time to time wake up and check it again
				try {
					threadManager.wait(); //from time to time thread is woken up to check the condition again
				} catch (InterruptedException e) {
					throw new RuntimeException("WatchDog was interrupted while was waiting for free workers for nodes in phase " + phase.getLabel());
				}
			}
			if (phase.getNodes().size() > 0) {
				//this barrier can be broken only when all components and watchdog is waiting there
				CyclicBarrier preExecuteBarrier = new CyclicBarrier(phase.getNodes().size() + 1);
				//this barrier is used for synchronization of all components between pre-execute and execute
				//it is necessary to finish all pre-execute's before execution
				CyclicBarrier executeBarrier = new CyclicBarrier(phase.getNodes().size());
				for (Node node: phase.getNodes().values()) {
					node.setPreExecuteBarrier(preExecuteBarrier);
					node.setExecuteBarrier(executeBarrier);
					threadManager.executeNode(node);
					logger.debug(node.getId()+ " ... starting");
				}
				try {
					//now we will wait for all components are really alive - node.getNodeThread() return non-null value
					preExecuteBarrier.await();
					logger.debug("All components are ready to start.");
				} catch (InterruptedException e) {
					throw new RuntimeException("WatchDog was interrupted while was waiting for workers startup in phase " + phase.getLabel());
				} catch (BrokenBarrierException e) {
					throw new RuntimeException("WatchDog or a worker was interrupted while was waiting for nodes tartup in phase " + phase.getLabel());
				}
			}
		}
	}

	/**
	 *  Description of the Method
	 *
	 * @param  phase  Description of the Parameter
	 * @return        Description of the Return Value
	 */
	protected Result executePhase(Phase phase) {
		currentPhase = phase;
		
		//preExecute() invocation
		try {
			phase.preExecute();
		} catch (Exception e) {
			ExceptionUtils.logException(logger, "Phase pre-execute initialization failed", e);
			setCauseException(e);
			if (e instanceof ComponentNotReadyException) {
				setCauseGraphElement(((ComponentNotReadyException) e).getGraphElement());
			}
			return Result.ERROR;
		}
		logger.info("Starting up all nodes in phase [" + phase.getLabel() + "]");
		startUpNodes(phase);

		logger.info("Successfully started all nodes in phase!");
		// watch running nodes in phase
		Result phaseStatus = Result.N_A;
        try{
            phaseStatus = watch(phase);
        } catch (InterruptedException ex){
            phaseStatus = Result.ABORTED;
        } finally {
        	
            //now we can notify all waiting phases for free threads
            synchronized (threadManager) {
            	threadManager.releaseNodeThreads(phase.getNodes().size());
                threadManager.notifyAll();
            }
            
            try {
	            //abort still running components - for failed graphs
	        	for (Node node : phase.getNodes().values()) {
	        		if (!node.getResultCode().isStop()) {
	    				if (logger.isTraceEnabled()) {
							logger.trace("Trying to abort node "+node);
	    				}
	        			node.abort();
	        		}
	        	}
            } finally {
	        	//postExecute() invocation
	        	try {
	        		phase.postExecute();
	        	} catch (Exception e) {
	        		ExceptionUtils.logException(logger, "Phase post-execute finalization failed", e);
	    			setCauseException(e);
	    			if (e instanceof ComponentNotReadyException) {
	    				setCauseGraphElement(((ComponentNotReadyException) e).getGraphElement());
	    			}
	    			phaseStatus = Result.ERROR;
	        	}
            }
        }
        
		return phaseStatus;
	}

	@Override
	public void sendMessage(Message<?> msg) {
        inMsgQueue.add(msg);
    }

    @Override
	public Message<?>[] receiveMessage(GraphElement recipient, final long wait) {
        Message<?>[] msg = null;
        synchronized (messageMonitor) {
            msg=(Message[])outMsgMap.get(recipient).toArray(new Message<?>[0]);
            if (msg!=null) {
                outMsgMap.remove(recipient);
            }
        }
        return msg;
    }

    @Override
	public boolean hasMessage(GraphElement recipient) {
        synchronized (messageMonitor ){
            return outMsgMap.containsKey(recipient);
        }
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
     * Sets cause exception of graph failure.
     * If some cause exception is already exists, {@link CompoundException} is created for both of them.
     * @param e
     */
    protected void setCauseException(Throwable e) {
    	//causeException = new ObfuscatingException(e);

    	if (causeException == null) {
        	causeException = e;
    	} else {
    		List<Throwable> causes = new ArrayList<Throwable>();
    		if (causeException instanceof CompoundException) {
    			causes.addAll(((CompoundException) causeException).getCauses());
    		} else {
    			causes.add(causeException);
    		}
    		causes.add(e);
    		causeException = new CompoundException(causes.toArray(new Throwable[causes.size()]));
    	}
    }

    /**
     * Returns ID of Node which caused
     * graph to stop processing.
     * 
     * @return the causeNodeID
     * @since 7.1.2007
     */
    public IGraphElement getCauseGraphElement() {
        return causeGraphElement;
    }

    protected void setCauseGraphElement(IGraphElement causeGraphElement) {
    	if (this.causeGraphElement == null) {
    		this.causeGraphElement = causeGraphElement;
    	}
    }
    
	public String getErrorMessage() {
    	return ExceptionUtils.getMessage(getCauseException());
    }
    
    /**
     * @return the graph
     * @since 26.2.2007
     */
    public TransformationGraph getTransformationGraph() {
        return graph;
    }

	@SuppressFBWarnings("IS2_INCONSISTENT_SYNC")
    public void setUseJMX(boolean useJMX) {
		this.provideJMX = useJMX;
	}

	public GraphRuntimeContext getGraphRuntimeContext() {
		return runtimeContext;
	}

	public boolean isFinishJMX() {
		return finishJMX;
	}

	public void setFinishJMX(boolean finishJMX) {
		this.finishJMX = finishJMX;
	}

	public IThreadManager getThreadManager() {
		return threadManager;
	}

	public void setThreadManager(IThreadManager threadManager) {
		this.threadManager = threadManager;
	}

	public TransformationGraph getGraph() {
		return graph;
	}
	
    public IAuthorityProxy getAuthorityProxy() {
    	return getGraphRuntimeContext().getAuthorityProxy();
    }

    public TokenTracker getTokenTracker() {
    	return tokenTracker;
    }
    
	public GraphTrackingDetail getGraphTracking() {
		return graphTracking;
	}

	public void setApprovedPhaseNumber(int approvedPhaseNumber) {
		this.approvedPhaseNumber = approvedPhaseNumber;
	}

	public WriterAppender getGraphLogAppender() {
		return graphLogAppender;
	}

	public void setGraphLogAppender(WriterAppender graphLogAppender) {
		this.graphLogAppender = graphLogAppender;
	}

    public void free() {
    	isReleased = true;
    }
    
	/**
	 * Prints information about conditionally enabled and all disabled components into log.
	 */
	private void printComponentsEnabledStatus() {
		//print information about conditionally enabled components
		boolean headerPrinted = false;
		for (Node component : graph.getNodes().values()) {
			String rawComponentEnabledAttribute = graph.getRawComponentEnabledAttribute().get(component);
			if (PropertyRefResolver.containsProperty(rawComponentEnabledAttribute)
					|| component.getEnabled().isDynamic()) {
				if (!headerPrinted) {
					logger.info("Enabled components (conditional only):");
					headerPrinted = true;
				}
				printSingleComponentEnabledStatus(component, rawComponentEnabledAttribute);
			}
		}
		
		headerPrinted = false;
		for (Node component : graph.getNodes().values()) {
			String rawComponentEnabledAttribute = graph.getRawComponentEnabledAttribute().get(component);
			if (component.getEnabled().isBlocker() || graph.getKeptBlockedComponents().contains(component)) {
				if (!headerPrinted) {
					logger.info("Components disabled as trash:");
					headerPrinted = true;
				}
				printSingleComponentEnabledStatus(component, rawComponentEnabledAttribute);
			}
		}
		
		//print information about disabled components
		headerPrinted = false;
		for (Node component : graph.getRawComponentEnabledAttribute().keySet()) {
			if (!component.getEnabled().isEnabled() ||
					(graph.getBlockedIDs().contains(component.getId()) && !graph.getKeptBlockedComponents().contains(component))) {
				
				if (!headerPrinted) {
					logger.info("Disabled components:");
					headerPrinted = true;
				}
				String rawComponentEnabledAttribute = graph.getRawComponentEnabledAttribute().get(component);
				printSingleComponentEnabledStatus(component, rawComponentEnabledAttribute);
			}
		}
	}

	@Override
	public String toString() {
		if (runtimeContext != null) {
			return "WatchDog-" + runtimeContext.getRunId();
		} else {
			return "WatchDog-???";
		}
	}
	/**
	 * Prints information about conditionally enabled and all disabled components into log.
	 */
	private void printSingleComponentEnabledStatus(Node component, String rawComponentEnabledAttribute) {
		StringBuilder sb = new StringBuilder("\t");
		sb.append(component);
		sb.append(" - ");
		if (graph.getBlockedIDs().contains(component.getId())) {
			if (graph.getKeptBlockedComponents().contains(component)) {
				sb.append("Trash mode: ");
			} else {
				sb.append(EnabledEnum.DISABLED.getLabel() + ": ");
			}
			sb.append("disabled by ");
			boolean needsDelim = false;
			Map<Node,Set<Node>> blockingInfo = graph.getBlockingComponentsInfo();
			for (Entry<Node, Set<Node>> blockerInfo : blockingInfo.entrySet()) {
				if (blockerInfo.getValue().contains(component)) {
					if (needsDelim) {
						sb.append(", ");
					}
					needsDelim = true;
					sb.append(blockerInfo.getKey().getId());
				}
			}
		} else if (EnabledEnum.TRASH.toString().equals(rawComponentEnabledAttribute)) {
			sb.append("Trash mode");
		} else {
			sb.append(component.getEnabled().isEnabled() ? EnabledEnum.ENABLED.getLabel() : EnabledEnum.DISABLED.getLabel());
			sb.append(": ");
			if (getGraphRuntimeContext().getJobType().isSubJob() && (component.isPartOfDebugInput() || component.isPartOfDebugOutput())) {
				sb.append("Part of subgraph debug area");
			} else if (PropertyRefResolver.isPropertyReference(rawComponentEnabledAttribute)) {
				String graphParameterName = PropertyRefResolver.getReferencedProperty(rawComponentEnabledAttribute);
				sb.append(graphParameterName);
				sb.append("=");
				sb.append(graph.getGraphParameters().getGraphParameter(graphParameterName).getValueResolved(null));
			} else if (PropertyRefResolver.containsProperty(rawComponentEnabledAttribute)) {
				sb.append(rawComponentEnabledAttribute);
				sb.append("=");
				sb.append(graph.getPropertyRefResolver().resolveRef(rawComponentEnabledAttribute));
			} else if (component.getEnabled().isDynamic()) {
				sb.append(component.getEnabled().getStatus());
			} else {
				sb.append("Always");
			}
		}
		logger.info(sb);
	}
	
	//******************* Tracking events ********************/
	
	public synchronized void graphStarted() {
		try {
			graphTracking.graphStarted();
		} catch (Exception e) {
			logger.error("Unexpected error during job tracking", e);
		}
		if (provideJMX) {
			Properties props = graph.getGraphParameters().asProperties();
			CloverJMX.getInstance().sendNotification(getGraphRuntimeContext().getRunId(), CloverJMXMBean.GRAPH_STARTED, null, props);
		}
	}

	public synchronized void phaseStarted(Phase phase) {
		try {
			graphTracking.phaseStarted(phase);
		} catch (Exception e) {
			logger.error("Unexpected error during job tracking", e);
		}
		
		if (provideJMX) {
			CloverJMX.getInstance().sendNotification(getGraphRuntimeContext().getRunId(), CloverJMXMBean.PHASE_STARTED);
		}
	}

	public synchronized void gatherTrackingDetails() {
		try {
			graphTracking.gatherTrackingDetails();
		} catch (Exception e) {
			logger.error("Unexpected error during job tracking", e);
		}
		
		if (provideJMX) {
			CloverJMX.getInstance().sendNotification(getGraphRuntimeContext().getRunId(), CloverJMXMBean.TRACKING_UPDATED);
		}
	}

	public synchronized void phaseFinished() {
		try {
			graphTracking.phaseFinished();
		} catch (Exception e) {
			logger.error("Unexpected error during job tracking", e);
		}

		if (provideJMX) {
			int runningPhaseNum = graphTracking.getRunningPhaseTracking().getPhaseNum();
			DictionaryValuesContainer dictionary = DictionaryValuesContainer.getDictionaryValuesContainer(graph.getDictionary(), true, true, true);
			JMXNotificationData data = new JMXNotificationData(runningPhaseNum, dictionary);
			CloverJMX.getInstance().sendNotification(getGraphRuntimeContext().getRunId(), CloverJMXMBean.PHASE_FINISHED, null, data);
		}
	}

	public synchronized void phaseAborted() {
		try {
			graphTracking.phaseFinished();
		} catch (Exception e) {
			logger.error("Unexpected error during job tracking", e);
		}
		
		if (provideJMX) {
			int runningPhaseNum = graphTracking.getRunningPhaseTracking().getPhaseNum();
			CloverJMX.getInstance().sendNotification(getGraphRuntimeContext().getRunId(), CloverJMXMBean.PHASE_ABORTED, null, runningPhaseNum);
		}
	}

	public synchronized void phaseError(String message) {
		try {
			graphTracking.phaseFinished();
		} catch (Exception e) {
			logger.error("Unexpected error during job tracking", e);
		}
		
		if (provideJMX) {
			int runningPhaseNum = graphTracking.getRunningPhaseTracking().getPhaseNum();
			CloverJMX.getInstance().sendNotification(getGraphRuntimeContext().getRunId(), CloverJMXMBean.PHASE_ERROR, null, runningPhaseNum);
		}
	}

	public synchronized void graphFinished() {
		try {
			graphTracking.graphFinished();
		} catch (Exception e) {
			logger.error("Unexpected error during job tracking", e);
		}

		if (provideJMX) {
			DictionaryValuesContainer dictionary = DictionaryValuesContainer.getDictionaryValuesContainer(graph.getDictionary(), false, true, false);
			CloverJMX.getInstance().sendNotification(getGraphRuntimeContext().getRunId(), CloverJMXMBean.GRAPH_FINISHED, null, dictionary);
		}
	}

	/**
	 * Graph was aborted. Only send a notification.
	 */
	public synchronized void graphAborted() {
		try {
			graphTracking.gatherTrackingDetails();
			graphTracking.graphFinished();
		} catch (Exception e) {
			logger.error("Unexpected error during job tracking", e);
		}

		if (provideJMX) {
			DictionaryValuesContainer dictionary = DictionaryValuesContainer.getDictionaryValuesContainer(graph.getDictionary(), false, true, false);
			CloverJMX.getInstance().sendNotification(getGraphRuntimeContext().getRunId(), CloverJMXMBean.GRAPH_ABORTED, null, dictionary);
		}
	}

	/**
	 * Graph ends with an error. Only send a notification.
	 */
	public synchronized void graphError(String message) {
		try {
			graphTracking.gatherTrackingDetails();
			graphTracking.graphFinished();
		} catch (Exception e) {
			logger.error("Unexpected error during job tracking", e);
		}

		if (provideJMX) {
			DictionaryValuesContainer dictionary = DictionaryValuesContainer.getDictionaryValuesContainer(graph.getDictionary(), false, true, false);
			CloverJMX.getInstance().sendNotification(getGraphRuntimeContext().getRunId(), CloverJMXMBean.GRAPH_ERROR, null, dictionary);
		}
	}
	
	public synchronized void nodeFinished(String message) {
		if (provideJMX) {
			CloverJMX.getInstance().sendNotification(getGraphRuntimeContext().getRunId(), CloverJMXMBean.NODE_FINISHED);
		}
	}
	
}

