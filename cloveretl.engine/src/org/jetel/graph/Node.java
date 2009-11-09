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
// FILE: c:/projects/jetel/org/jetel/graph/Node.java

package org.jetel.graph;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.CyclicBarrier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.MDC;
import org.jetel.data.DataRecord;
import org.jetel.enums.EnabledEnum;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.runtime.CloverPost;
import org.jetel.graph.runtime.ErrorMsgBody;
import org.jetel.graph.runtime.Message;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  A class that represents atomic transformation task. It is a base class for
 *  all kinds of transformation components.
 *
 *@author      D.Pavlis
 *@created     January 31, 2003
 *@since       April 2, 2002
 *@see         org.jetel.component
 *@revision    $Revision$
 */
public abstract class Node extends GraphElement implements Runnable {

    private static final Log logger = LogFactory.getLog(Node.class);

    protected Thread nodeThread;
    /**
     * List of all threads under this component.
     * For instance parallel reader uses threads for parallel reading.
     * It is component's responsibility to register all inner threads via addChildThread() method.
     */
    protected List<Thread> childThreads;  
    protected EnabledEnum enabled;
    protected int passThroughInputPort;
    protected int passThroughOutputPort;
    
	protected TreeMap<Integer, OutputPort> outPorts;
	protected TreeMap<Integer, InputPort> inPorts;

	protected OutputPort logPort;

	protected volatile boolean runIt = true;

	protected volatile Result runResult;
    protected Throwable resultException;
    protected String resultMessage;
	
    protected Phase phase;

    // buffered values
    protected OutputPort[] outPortsArray;
    protected int outPortsSize;
    
    //synchronization barrier for synchronization randezvous all nodes after pre-execute
    //execute() method can be invoked only after successful preExecute() methods on all nodes in the phase
    private CyclicBarrier preExecuteBarrier;
    
	/**
	 *  Various PORT kinds identifiers
	 *
	 *@since    August 13, 2002
	 */
	public final static char OUTPUT_PORT = 'O';
	/**  Description of the Field */
	public final static char INPUT_PORT = 'I';
	/**  Description of the Field */
	public final static char LOG_PORT = 'L';

	/**
	 * XML attributes of every cloverETL component
	 */
	public final static String XML_TYPE_ATTRIBUTE="type";
    public final static String XML_ENABLED_ATTRIBUTE="enabled";

    /**
     *  Standard constructor.
     *
     *@param  id  Unique ID of the Node
     *@since      April 4, 2002
     */
    public Node(String id){
        this(id,null);
    }
    
    /**
	 *  Standard constructor.
	 *
	 *@param  id  Unique ID of the Node
	 *@since      April 4, 2002
	 */
	public Node(String id, TransformationGraph graph) {
		super(id,graph);
		outPorts = new TreeMap<Integer, OutputPort>();
		inPorts = new TreeMap<Integer, InputPort>();
		logPort = null;
        phase = null;
        runResult=Result.N_A; // result is not known yet
        childThreads = new ArrayList<Thread>();
	}

	/**
	 *  Sets the EOF for particular output port. EOF indicates that no more data
	 * will be sent throught the output port.
	 *
	 *@param  portNum  The new EOF value
	 * @throws IOException 
	 * @throws IOException 
	 *@since           April 18, 2002
	 */
	public void setEOF(int portNum) throws InterruptedException, IOException {
		try {
			((OutputPort) outPorts.get(Integer.valueOf(portNum))).eof();
		} catch (IndexOutOfBoundsException ex) {
			ex.printStackTrace();
		}
	}

	/**
	 *  Returns the type of this Node (subclasses/Components should override
	 * this method to return appropriate type).
	 *
	 *@return    The Type value
	 *@since     April 4, 2002
	 */
	public abstract String getType();

	/**
	 *  Returns True if this Node is Leaf Node - i.e. only consumes data (has only
	 * input ports connected to it)
	 *
	 *@return    True if Node is a Leaf
	 *@since     April 4, 2002
	 */
	public boolean isLeaf() {
		if (outPorts.size() == 0) {
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * Returns True if this Node is Phase Leaf Node - i.e. only consumes data within
	 * phase it belongs to (has only input ports connected or any connected output ports
	 * connects this Node with Node in different phase)
	 * 
	 * @return True if this Node is Phase Leaf
	 */
	public boolean isPhaseLeaf() {
		for (OutputPort outputPort : getOutPorts()) {
			if (phase != outputPort.getReader().getPhase()) {
				return true;
			}
		}
		return false;
	}

	/**
	 *  Returns True if this node is Root Node - i.e. it produces data (has only output ports
	 * connected to id).
	 *
	 *@return    True if Node is a Root
	 *@since     April 4, 2002
	 */
	public boolean isRoot() {
		if (inPorts.size() == 0) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 *  Sets the processing phase of the Node object.<br>
	 *  Default is 0 (ZERO).
	 *
	 *@param  phase  The new phase number
	 */
	public void setPhase(Phase phase) {
		this.phase = phase;
	}

	/**
	 *  Gets the processing phase of the Node object
	 *
	 *@return    The phase value
	 */
	public Phase getPhase() {
		return phase;
	}

    /**
     * @return phase number
     */
    public int getPhaseNum(){
        return phase.getPhaseNum();
    }
    
	/**
	 *  Gets the OutPorts attribute of the Node object
	 *
	 *@return    Collection of OutPorts
	 *@since     April 18, 2002
	 */
	public Collection<OutputPort> getOutPorts() {
		return outPorts.values();
	}

	/**
	 *  Gets the InPorts attribute of the Node object
	 *
	 *@return    Collection of InPorts
	 *@since     April 18, 2002
	 */
	public Collection<InputPort> getInPorts() {
		return inPorts.values();
	}

	/**
	 *  Gets the metadata on output ports of the Node object
	 *
	 *@return    Collection of output ports metadata
	 */
	public List<DataRecordMetadata> getOutMetadata() {
		List<DataRecordMetadata> ret = new ArrayList<DataRecordMetadata>(outPorts.size());
		for(Iterator<OutputPort> it = getOutPorts().iterator(); it.hasNext();) {
		    ret.add(it.next().getMetadata());
		}
	    return ret;
	}

	/**
	 *  Gets the metadata on input ports of the Node object
	 *
	 *@return    Collection of input ports metadata
	 */
	public List<DataRecordMetadata> getInMetadata() {
		List<DataRecordMetadata> ret = new ArrayList<DataRecordMetadata>(inPorts.size());
		for(Iterator<InputPort> it = getInPorts().iterator(); it.hasNext();) {
		    ret.add(it.next().getMetadata());
		}
	    return ret;
	}

	/**
	 *  Gets the number of records passed through specified port type and number
	 *
	 *@param  portType  Port type (IN, OUT, LOG)
	 *@param  portNum   port number (0...)
	 *@return           The RecordCount value
	 *@since            May 17, 2002
     *@deprecated
	 */
	public int getRecordCount(char portType, int portNum) {
		int count;
        // Integer used as key to TreeMap containing ports
		Integer port = Integer.valueOf(portNum);
		try {
			switch (portType) {
				case OUTPUT_PORT:
					count = ((OutputPort) outPorts.get(port)).getOutputRecordCounter();
					break;
				case INPUT_PORT:
					count = ((InputPort) inPorts.get(port)).getInputRecordCounter();
					break;
				case LOG_PORT:
					if (logPort != null) {
						count = logPort.getOutputRecordCounter();
					} else {
						count = -1;
					}
					break;
				default:
					count = -1;
			}
		} catch (Exception ex) {
			count = -1;
		}

		return count;
	}

	/**
	 *  Gets the result code of finished Node.<br>
	 *
	 *@return    The Result value
	 *@since     July 29, 2002
     *@see       org.jetel.graph.Node.Result
	 */
	public Result getResultCode() {
		return runResult;
	}

	/**
	 * Sets the result code of component.
	 * @param result
	 */
	public void setResultCode(Result result) {
		this.runResult = result;
	}

	/**
	 *  Gets the ResultMsg of finished Node.<br>
	 *  This message briefly describes what caused and error (if there was any).
	 *
	 *@return    The ResultMsg value
	 *@since     July 29, 2002
	 */
	public String getResultMsg() {
		return runResult!=null ? runResult.message() : null;
	}

    /**
     * Gets exception which caused Node to fail execution - if
     * there was such failure.
     * 
     * @return
     * @since 13.12.2006
     */
    public Throwable getResultException(){
        return resultException;
    }

	// Operations

    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#init()
     */
    @Override 
    public void init() throws ComponentNotReadyException {
        super.init();

        runResult = Result.READY;
        refreshBufferedValues();
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#preExecute()
     */
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
    	
        runResult = Result.RUNNING;
    }
    
	/**
	 *  main execution method of Node (calls in turn execute())
	 *
	 *@since    April 2, 2002
	 */
	public void run() {
        runResult = Result.RUNNING; // set running result, so we know run() method was started
        
		//store the current thread like a node executor
        setNodeThread(Thread.currentThread());
		
        try {
    		//preExecute() invocation
    		try {
    			preExecute();
    		} catch (ComponentNotReadyException e) {
    			throw new ComponentNotReadyException(this, "Component pre-execute initialization failed.", e);
    		}

    		//waiting for other nodes in the current phase - first all pre-execution has to be done at all nodes
    		preExecuteBarrier.await();
    		
    		//execute() invocation
            if((runResult = execute()) == Result.ERROR) {
                Message<ErrorMsgBody> msg = Message.createErrorMessage(this,
                        new ErrorMsgBody(runResult.code(), 
                                resultMessage != null ? resultMessage : runResult.message(), null));
                getCloverPost().sendMessage(msg);
            }
            
            if (runResult == Result.FINISHED_OK) {
            	//check whether all input ports are already closed
            	for (InputPort inputPort : getInPorts()) {
            		if (!inputPort.isEOF()) {
            			runResult = Result.ERROR;
            			Message<ErrorMsgBody> msg = Message.createErrorMessage(this,
            					new ErrorMsgBody(runResult.code(), "Component has finished and input port " + inputPort.getInputPortNumber() + " still contains some unread records.", null));
            			getCloverPost().sendMessage(msg);
            			return;
            		}
            	}
            	//broadcast all output ports with EOF information
            	broadcastEOF();
            }
        } catch (InterruptedException ex) {
            runResult=Result.ABORTED;
            return;
        } catch (IOException ex) {  // may be handled differently later
            runResult=Result.ERROR;
            resultException = ex;
            Message<ErrorMsgBody> msg = Message.createErrorMessage(this,
                    new ErrorMsgBody(runResult.code(), runResult.message(), ex));
            getCloverPost().sendMessage(msg);
            return;
        } catch (TransformException ex){
            runResult=Result.ERROR;
            resultException = ex;
            Message<ErrorMsgBody> msg = Message.createErrorMessage(this,
                    new ErrorMsgBody(runResult.code(), "Error occurred in nested transformation: " + runResult.message(), ex));
            getCloverPost().sendMessage(msg);
            return;
        } catch (SQLException ex){
            runResult=Result.ERROR;
            resultException = ex;
            Message<ErrorMsgBody> msg = Message.createErrorMessage(this,
                    new ErrorMsgBody(runResult.code(), runResult.message(), ex));
            getCloverPost().sendMessage(msg);
            return;
        } catch (Exception ex) { // may be handled differently later
            runResult=Result.ERROR;
            resultException = ex;
            Message<ErrorMsgBody> msg = Message.createErrorMessage(this,
                    new ErrorMsgBody(runResult.code(), runResult.message(), ex));
            getCloverPost().sendMessage(msg);
            return;
        } finally {
        	sendFinishMessage();

        	setNodeThread(null);
        }
    }
    
    public abstract Result execute() throws Exception;
    
    /**
     * This method should be called every time when node finishes its work.
     */
    private void sendFinishMessage() {
        //sends notification - node has finished
        Message<Object> msg = Message.createNodeFinishedMessage(this);
        if (getCloverPost() != null) //that condition should be removed - graph aborting is not well synchronized now
        	getCloverPost().sendMessage(msg);
    }
    
	/**
	 *  Abort execution of Node - only inform node, that should finish processing.
	 *
	 *@since    April 4, 2002
	 */
	synchronized public void abort() {
		int attempts = 30;
		runIt = false;
		while (runResult == Result.RUNNING && attempts-- > 0){
			getNodeThread().interrupt();
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
			}
		}
		if (runResult == Result.RUNNING) {
			logger.warn("Node '" + getId() + "' was not interrupted in legal way.");
			runResult = Result.ABORTED;
			sendFinishMessage();
		}
	}

    /**
     * @return thread of running node; <b>null</b> if node does not running
     */
    public synchronized Thread getNodeThread() {
        return nodeThread;
    }

    /**
     * Sets actual thread in which this node current running.
     * @param nodeThread
     */
    private synchronized void setNodeThread(Thread nodeThread) {
		if(nodeThread != null) {
    		this.nodeThread = nodeThread;

    		ContextProvider.registerNode(this);
			MDC.put("runId", getGraph().getRuntimeContext().getRunId());
			nodeThread.setName(getId());
		} else {
			ContextProvider.unregister();
			MDC.remove("runId");
			this.nodeThread.setName("<unnamed>");
		}

		notifyAll(); //we have to notify all threads waiting on startup (watchdog) - the component is already running and the thread is preset
					// see method waitForStartup()
    }
    
	/**
	 * This is blocking method, current thread is waiting for the node is running.
	 */
	public synchronized void waitForStartup() {
		while (getNodeThread() == null) { //we have to wait to real start up of the node
			try {
				wait();
			} catch (InterruptedException e) {
				throw new RuntimeException("Unexpected interruption of waiting for startup of node '" + getName() + "'.");
			}
		}
	}

	/**
	 *  End execution of Node - let Node finish gracefully
	 *
	 *@since    April 4, 2002
	 */
	public void end() {
		runIt = false;
	}

    /**
     * Provides CloverRuntime - object providing
     * various run-time services
     * 
     * @return
     * @since 13.12.2006
     */
    public CloverPost getCloverPost(){
        return getGraph().getPost();
    }
    
	/**
	 *  An operation that adds port to list of all InputPorts
	 *
	 *@param  port   Port (Input connection) to be added
	 *@since         April 2, 2002
	 *@deprecated    Use the other method which takes 2 arguments (portNum, port)
	 */
	public void addInputPort(InputPort port) {
		Integer portNum;
		int keyVal;
		try {
			portNum = (Integer) inPorts.lastKey();
			keyVal = portNum.intValue() + 1;
		} catch (NoSuchElementException ex) {
			keyVal = 0;
		}
		inPorts.put(Integer.valueOf(keyVal), port);
		port.connectReader(this, keyVal);
	}

	/**
	 *  An operation that adds port to list of all InputPorts
	 *
	 *@param  portNum  Number to be associated with this port
	 *@param  port     Port (Input connection) to be added
	 *@since           April 2, 2002
	 */
	public void addInputPort(int portNum, InputPort port) {
		inPorts.put(Integer.valueOf(portNum), port);
		port.connectReader(this, portNum);
	}

	/**
	 *  An operation that adds port to list of all OutputPorts
	 *
	 *@param  port   Port (Output connection) to be added
	 *@since         April 4, 2002
	 *@deprecated    Use the other method which takes 2 arguments (portNum, port)
	 */
	public void addOutputPort(OutputPort port) {
		Integer portNum;
		int keyVal;
		try {
			portNum = (Integer) inPorts.lastKey();
			keyVal = portNum.intValue() + 1;
		} catch (NoSuchElementException ex) {
			keyVal = 0;
		}
		outPorts.put(Integer.valueOf(keyVal), port);
		port.connectWriter(this, keyVal);
        resetBufferedValues();
	}

	/**
	 *  An operation that adds port to list of all OutputPorts
	 *
	 *@param  portNum  Number to be associated with this port
	 *@param  port     The feature to be added to the OutputPort attribute
	 *@since           April 4, 2002
	 */
	public void addOutputPort(int portNum, OutputPort port) {
		outPorts.put(Integer.valueOf(portNum), port);
		port.connectWriter(this, portNum);
        resetBufferedValues();
	}

	/**
	 *  Gets the port which has associated the num specified
	 *
	 *@param  portNum  number associated with the port
	 *@return          The outputPort
	 */
	public OutputPort getOutputPort(int portNum) {
        Object outPort=outPorts.get(Integer.valueOf(portNum));
        if (outPort instanceof OutputPort) { 
            return (OutputPort)outPort ;
        }else if (outPort==null) {
            return null;
        }
        throw new RuntimeException("Port number \""+portNum+"\" does not implement OutputPort interface "+outPort.getClass().getName());
	}

    /**
     *  Gets the port which has associated the num specified
     *
     *@param  portNum  number associated with the port
     *@return          The outputPort
     */
    public OutputPortDirect getOutputPortDirect(int portNum) {
        Object outPort=outPorts.get(Integer.valueOf(portNum));
        if (outPort instanceof OutputPortDirect) {
            return (OutputPortDirect)outPort ;
        }else if (outPort==null) {
            return null;
        }
        throw new RuntimeException("Port number \""+portNum+"\" does not implement OutputPortDirect interface");
    }

	/**
	 *  Gets the port which has associated the num specified
	 *
	 *@param  portNum  portNum number associated with the port
	 *@return          The inputPort
	 */
	public InputPort getInputPort(int portNum) {
        Object inPort=inPorts.get(Integer.valueOf(portNum));
        
        if (inPort instanceof InputPort) {
            return (InputPort)inPort ;
        }else if (inPort==null){
            return null;
        }
        throw new RuntimeException("Port number \""+portNum+"\" does not implement InputPort interface");
	}

    /**
     *  Gets the port which has associated the num specified
     *
     *@param  portNum  portNum number associated with the port
     *@return          The inputPort
     */
    public InputPortDirect getInputPortDirect(int portNum) {
        Object inPort=inPorts.get(Integer.valueOf(portNum));
        if (inPort instanceof InputPortDirect) {
            return (InputPortDirect)inPort ;
        }else if (inPort==null){
            return null;
        }
        throw new RuntimeException("Port number \""+portNum+"\" does not implement InputPortDirect interface");
    }
    
    /**
     * Removes input port.
     * @param inputPort
     */
    public void removeInputPort(InputPort inputPort) {
        inPorts.remove(Integer.valueOf(inputPort.getInputPortNumber()));
    }

    /**
     * Removes output port.
     * @param outputPort
     */
    public void removeOutputPort(OutputPort outputPort) {
        outPorts.remove(Integer.valueOf(outputPort.getOutputPortNumber()));
        resetBufferedValues();
    }

	/**
	 *  Adds a feature to the LogPort attribute of the Node object
	 *
	 *@param  port  The feature to be added to the LogPort attribute
	 *@since        April 4, 2002
	 */
	public void addLogPort(OutputPort port) {
		logPort = port;
		port.connectWriter(this, -1);
	}

	/**
	 *  An operation that does removes/unregisteres por<br>
	 *  Not yet implemented.
	 *
	 *@param  _portNum   Description of Parameter
	 *@param  _portType  Description of Parameter
	 *@since             April 2, 2002
	 */
	public void deletePort(int _portNum, char _portType) {
        throw new UnsupportedOperationException("Deleting port is not supported !");
	}

	/**
	 *  An operation that writes one record through specified output port.<br>
     *  As this operation gets the Port object from TreeMap, don't use it in loops
     *  or when time is critical. Instead obtain the Port object directly and
     *  use it's writeRecord() method.
	 *
	 *@param  _portNum                  Description of Parameter
	 *@param  _record                   Description of Parameter
	 *@exception  IOException           Description of Exception
	 *@exception  InterruptedException  Description of Exception
	 *@since                            April 2, 2002
	 */
	public void writeRecord(int _portNum, DataRecord _record) throws IOException, InterruptedException {
			((OutputPort) outPorts.get(Integer.valueOf(_portNum))).writeRecord(_record);
	}

	/**
	 *  An operation that reads one record through specified input port.<br>
     *  As this operation gets the Port object from TreeMap, don't use it in loops
     *  or when time is critical. Instead obtain the Port object directly and
     *  use it's readRecord() method.
	 *
	 *@param  _portNum                  Description of Parameter
	 *@param  record                    Description of Parameter
	 *@return                           Description of the Returned Value
	 *@exception  IOException           Description of Exception
	 *@exception  InterruptedException  Description of Exception
	 *@since                            April 2, 2002
	 */
	public DataRecord readRecord(int _portNum, DataRecord record) throws IOException, InterruptedException {
		return	((InputPort) inPorts.get(Integer.valueOf(_portNum))).readRecord(record);
	}

	/**
	 *  An operation that writes record to Log port
	 *
	 *@param  record                    Description of Parameter
	 *@exception  IOException           Description of Exception
	 *@exception  InterruptedException  Description of Exception
	 *@since                            April 2, 2002
	 */
	public void writeLogRecord(DataRecord record) throws IOException, InterruptedException {
			logPort.writeRecord(record);
	}

	/**
	 *  An operation that does ...
	 *
	 *@param  record                    Description of Parameter
	 *@exception  IOException           Description of Exception
	 *@exception  InterruptedException  Description of Exception
	 *@since                            April 2, 2002
	 */
	public void writeRecordBroadcast(DataRecord record) throws IOException, InterruptedException {
        for(int i=0;i<outPortsSize;i++){
				outPortsArray[i].writeRecord(record);
		}
	}

	/**
	 *  Description of the Method
	 *
	 *@param  recordBuffer              Description of Parameter
	 *@exception  IOException           Description of Exception
	 *@exception  InterruptedException  Description of Exception
	 *@since                            August 13, 2002
	 */
    public void writeRecordBroadcastDirect(ByteBuffer recordBuffer) throws IOException, InterruptedException {
        for(int i=0;i<outPortsSize;i++){
            ((OutputPortDirect) outPortsArray[i]).writeRecordDirect(recordBuffer);
            recordBuffer.rewind();
        }
    }

	/**
	 *  Closes all output ports - sends EOF signal to them.
	 * @throws IOException 
	 *
	 *@since    April 11, 2002
	 */
	public void closeAllOutputPorts() throws InterruptedException, IOException {
		for (OutputPort outputPort : getOutPorts()) {
			outputPort.eof();
		}
	}

	/**
	 *  Send EOF (no more data) to all connected output ports
	 * @throws IOException 
	 *
	 *@since    April 18, 2002
	 */
	public void broadcastEOF() throws InterruptedException, IOException{
		closeAllOutputPorts();
	}

	/**
	 *  Closes specified output port - sends EOF signal. 
	 *
	 *@param  portNum  Which port to close
	 * @throws IOException 
	 *@since           April 11, 2002
	 */
	public void closeOutputPort(int portNum) throws InterruptedException, IOException {
        OutputPort port = (OutputPort) outPorts.get(Integer.valueOf(portNum));
        if (port == null) {
            throw new RuntimeException(this.getId()+" - can't close output port \"" + portNum
                    + "\" - does not exists!");
        }
        port.eof();
    }

	/**
	 * Compares this Node to specified Object
	 * 
	 * @param obj
	 *            Node to compare with
	 * @return True if obj represents node with the same ID
	 * @since April 18, 2002
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof Node)) {
			return false;
		}
		final Node other = (Node) obj;

		return getId().equals(other.getId());
	}

    @Override 
    public int hashCode(){
        return getId().hashCode();
    }

	/**
	 *  Description of the Method
	 *
	 *@return    Description of the Returned Value
	 *@since     May 21, 2002
	 */
	public void toXML(Element xmlElement) {
		// set basic XML attributes of all graph components
		xmlElement.setAttribute(XML_ID_ATTRIBUTE, getId());
		xmlElement.setAttribute(XML_TYPE_ATTRIBUTE, getType());
	}

	/**
	 *  Description of the Method
	 *
	 *@param  nodeXML  Description of Parameter
	 *@return          Description of the Returned Value
	 *@since           May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement)throws XMLConfigurationException {
        throw new  UnsupportedOperationException("not implemented in org.jetel.graph.Node"); 
	}

    /**
     * @return <b>true</b> if node is enabled; <b>false</b> else
     */
    public EnabledEnum getEnabled() {
        return enabled;
    }

    /**
     * @param enabled whether node is enabled
     */
    public void setEnabled(String enabledStr) {
        enabled = EnabledEnum.fromString(enabledStr, EnabledEnum.ENABLED);
    }

    /**
     * @return index of "pass through" input port
     */
    public int getPassThroughInputPort() {
        return passThroughInputPort;
    }

    /**
     * Sets "pass through" input port.
     * @param passThroughInputPort
     */
    public void setPassThroughInputPort(int passThroughInputPort) {
        this.passThroughInputPort = passThroughInputPort;
    }

    /**
     * @return index of "pass through" output port
     */
    public int getPassThroughOutputPort() {
        return passThroughOutputPort;
    }

    /**
     * Sets "pass through" output port
     * @param passThroughOutputPort
     */
    public void setPassThroughOutputPort(int passThroughOutputPort) {
        this.passThroughOutputPort = passThroughOutputPort;
    }
    
    protected void resetBufferedValues(){
        outPortsArray=null;
        outPortsSize=0;
    }
    
    protected void refreshBufferedValues(){
        Collection<OutputPort> op = getOutPorts();
        outPortsArray = (OutputPort[]) op.toArray(new OutputPort[op.size()]);
        outPortsSize = outPortsArray.length;
    }
    
    /**
     * Checks number of input ports, whether is in the given interval.
     * @param status
     * @param min
     * @param max
     * @return true if the number of input ports is in the given interval; else false
     */
    protected boolean checkInputPorts(ConfigurationStatus status, int min, int max) {
    	boolean retValue = true;
    	Collection<InputPort> inPorts = getInPorts();
        if(inPorts.size() < min) {
            status.add(new ConfigurationProblem("At least " + min + " input port must be defined!", Severity.ERROR, this, Priority.NORMAL));
            retValue = false;
        }
        if(inPorts.size() > max) {
            status.add(new ConfigurationProblem("At most " + max + " input ports can be defined!", Severity.ERROR, this, Priority.NORMAL));
            retValue = false;
        }

        int index = 0;
        for (InputPort inputPort : inPorts) {
			if (inputPort.getMetadata() == null){ //TODO interface for matadata
                status.add(new ConfigurationProblem("Metadata on input port " + inputPort.getInputPortNumber() + 
                		" are not defined!", Severity.WARNING, this, Priority.NORMAL));
                retValue = false;
			}
			if (inputPort.getInputPortNumber() != index){
                status.add(new ConfigurationProblem("Input port " + index + " is not defined!", Severity.WARNING, this, Priority.NORMAL));
                retValue = false;
			}
			index++;
		}
        
        return retValue;
    }

    /**
     * Checks number of output ports, whether is in the given interval.
     * @param status
     * @param min
     * @param max
     * @return true if the number of output ports is in the given interval; else false
     */
    protected boolean checkOutputPorts(ConfigurationStatus status, int min, int max) {
    	Collection<OutputPort> outPorts = getOutPorts();
        if(outPorts.size() < min) {
            status.add(new ConfigurationProblem("At least " + min + " output port must be defined!", Severity.ERROR, this, Priority.NORMAL));
            return false;
        }
        if(outPorts.size() > max) {
            status.add(new ConfigurationProblem("At most " + max + " output ports can be defined!", Severity.ERROR, this, Priority.NORMAL));
            return false;
        }
        int index = 0;
        for (OutputPort outputPort : outPorts) {
			if (outputPort.getMetadata() == null){
                status.add(new ConfigurationProblem("Metadata on output port " + outputPort.getOutputPortNumber() + 
                		" are not defined!", Severity.WARNING, this, Priority.NORMAL));
                return false;
			}
			if (outputPort.getOutputPortNumber() != index){
                status.add(new ConfigurationProblem("Output port " + index + " is not defined!", Severity.WARNING, this, Priority.NORMAL));
                return false;
			}
			index++;
		}

        return true;
    }
    
    /**
     * Checks if metadatas in given list are all equal 
     * 
     * @param status
     * @param metadata list of metadata to check
     * @return
     */
    protected ConfigurationStatus checkMetadata(ConfigurationStatus status, Collection<DataRecordMetadata> metadata){
    	return checkMetadata(status, metadata, (Collection<DataRecordMetadata>)null);
    }

    /**
     * Checks if all metadata (in inMetadata list as well as in outMetadata list) are equal
     * 
     * @param status
     * @param inMetadata
     * @param outMetadata
     * @return
     */
    protected ConfigurationStatus checkMetadata(ConfigurationStatus status, Collection<DataRecordMetadata> inMetadata,
    		Collection<DataRecordMetadata> outMetadata){
    	return checkMetadata(status, inMetadata, outMetadata, true);
    }
    
    /**
     * Checks if all metadata (in inMetadata list as well as in outMetadata list) are equal
	 * If checkFixDelType is true then checks fixed/delimited state.
     * 
     * @param status
     * @param inMetadata
     * @param outMetadata
     * @param checkFixDelType
     * @return
     */
    protected ConfigurationStatus checkMetadata(ConfigurationStatus status, Collection<DataRecordMetadata> inMetadata,
    		Collection<DataRecordMetadata> outMetadata, boolean checkFixDelType){
    	Iterator<DataRecordMetadata> iterator = inMetadata.iterator();
    	DataRecordMetadata metadata = null, nextMetadata;
    	if (iterator.hasNext()) {
    		metadata = iterator.next();
    	}
    	//check input metadata
    	while (iterator.hasNext()) {
    		nextMetadata = iterator.next();
    		if (metadata == null || !metadata.equals(nextMetadata)) {
    			status.add(new ConfigurationProblem("Metadata " + 
	    					StringUtils.quote(metadata == null ? "null" : metadata.getName()) + 
	    					" does not equal to metadata " + 
	    					StringUtils.quote(nextMetadata == null ? "null" : nextMetadata.getName()), 
    					metadata == null || nextMetadata == null ? Severity.WARNING : Severity.ERROR, 
    					this, Priority.NORMAL));
    		}
    		metadata = nextMetadata;
    	}
    	if (outMetadata == null) {
    		return status;
    	}
    	//check if input metadata equals output metadata
    	iterator = outMetadata.iterator();
    	if (iterator.hasNext()) {
    		nextMetadata = iterator.next();
    		if (metadata == null || !metadata.equals(nextMetadata, checkFixDelType)) {
    			status.add(new ConfigurationProblem("Metadata " + 
    					StringUtils.quote(metadata == null ? "null" : metadata.getName()) + 
    					" does not equal to metadata " + 
    					StringUtils.quote(nextMetadata == null ? "null" : nextMetadata.getName()), 
					metadata == null || nextMetadata == null ? Severity.WARNING : Severity.ERROR, 
					this, Priority.NORMAL));
    		}
    		metadata = nextMetadata;
    	}
    	//check output metadata
    	while (iterator.hasNext()) {
    		nextMetadata = iterator.next();
    		if (metadata == null || !metadata.equals(nextMetadata)) {
    			status.add(new ConfigurationProblem("Metadata " + 
    					StringUtils.quote(metadata == null ? "null" : metadata.getName()) + 
    					" does not equal to metadata " + 
    					StringUtils.quote(nextMetadata == null ? "null" : nextMetadata.getName()), 
					metadata == null || nextMetadata == null ? Severity.WARNING : Severity.ERROR, 
					this, Priority.NORMAL));
    		}
    		metadata = nextMetadata;
    	}
    	return status;
    }
    
    protected ConfigurationStatus checkMetadata(ConfigurationStatus status, DataRecordMetadata inMetadata, 
    		Collection<DataRecordMetadata> outMetadata){
    	Collection<DataRecordMetadata> inputMetadata = new ArrayList<DataRecordMetadata>(1);
    	inputMetadata.add(inMetadata);
    	return checkMetadata(status, inputMetadata, outMetadata);
    }
    
    protected ConfigurationStatus checkMetadata(ConfigurationStatus status, Collection<DataRecordMetadata> inMetadata, 
    		DataRecordMetadata outMetadata){
    	Collection<DataRecordMetadata> outputMetadata = new ArrayList<DataRecordMetadata>(1);
    	outputMetadata.add(outMetadata);
    	return checkMetadata(status, inMetadata, outputMetadata);
    }

    protected ConfigurationStatus checkMetadata(ConfigurationStatus status, DataRecordMetadata inMetadata, 
    		DataRecordMetadata outMetadata) {
		Collection<DataRecordMetadata> inputMetadata = new ArrayList<DataRecordMetadata>(1);
		inputMetadata.add(inMetadata);
		Collection<DataRecordMetadata> outputMetadata = new ArrayList<DataRecordMetadata>(1);
		outputMetadata.add(outMetadata);
		return checkMetadata(status, inputMetadata, outputMetadata);
	}

    /**
     * The given thread is registered as a child thread of this component.
     * The child threads are exploited for grabing of tracking information - CPU usage of this component
     * is sum of all threads.
     * @param childThread
     */
    protected void registerChildThread(Thread childThread) {
    	childThreads.add(childThread);
    }

    /**
     * The given threads are registered as child threads of this component.
     * The child threads are exploited for grabing of tracking information - for instance 
     * CPU usage of this component is sum of all threads.
     * @param childThreads
     */
    protected void registerChildThreads(List<Thread> childThreads) {
    	childThreads.addAll(childThreads);
    }

    /**
     * @return list of all child threads - threads running under this component
     */
    public List<Thread> getChildThreads() {
    	return childThreads;
    }


    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#reset()
     * @deprecated see {@link org.jetel.graph.IGraphElement#preExecute()} and {@link org.jetel.graph.IGraphElement#postExecute()} methods 
     */
    @Deprecated
    synchronized public void reset() throws ComponentNotReadyException {
    	super.reset();
        for(OutputPort outPort : this.getOutPorts())
        	outPort.reset();
        for(InputPort inPort : this.getInPorts())
        	inPort.reset();
        if (logPort!=null)
        	logPort.reset();
    	runIt = true;
        runResult=Result.READY;
        resultMessage = null;
        resultException = null;
        childThreads.clear();
        nodeThread = null;
    }

    @Override
    public synchronized void free() {
    	super.free();
    	
    	childThreads.clear();
    }

	/**
	 * This method is intended to be overridden.
	 * @return URLs which this component is using
	 */
    public String[] getUsedUrls() {
    	return new String[0];
    }

	public void setPreExecuteBarrier(CyclicBarrier preExecuteBarrier) {
		this.preExecuteBarrier = preExecuteBarrier;
	}

}
