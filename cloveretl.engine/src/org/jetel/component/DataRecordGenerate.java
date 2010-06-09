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
package org.jetel.component;

import java.util.Properties;

import org.jetel.data.DataRecord;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.sequence.Sequence;
import org.jetel.database.IConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransactionMethod;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * Base class used for generating any kind of data generation. Subclass this one to create your own generations.
 * It implements basic stubs for all of the RecordTransform interface methods.<br>
 * <h4>Order of execution/methods call</h4>
 * <ol>
 * <li>setGraph()</li>
 * <li>init()</li>
 * <li>generate() <i>for each input&amp;output records pair</i></li>
 * <li><i>optionally</i> getMessage() <i>or</i> signal() <i>or</i> getSemiResult()</li>
 * <li>finished()
 * </ol>
 * 
 * @author dpavlis
 * @since November 1, 2003
 * @revision $Revision: 5158 $
 * @created November 1, 2003
 * @see org.jetel.component.RecordTransform
 */
@SuppressWarnings("EI")
public abstract class DataRecordGenerate implements RecordGenerate {

	protected TransformationGraph graph;

	protected String generateName;
	/**
	 * Use <code>errorMessage</code> to report details of problems which occured within generate method.<br>
	 * Ideally, within catch() section assign meaningful message to errorMessage field.
	 */
	protected String errorMessage;

	protected Properties parameters;
	protected DataRecordMetadata[] targetMetadata;

	/** Constructor for the DataRecordGenerate object */
	public DataRecordGenerate() {
		this(null, "anonymous");
	}

	/** Constructor for the DataRecordGenerate object */
	public DataRecordGenerate(TransformationGraph graph) {
		this(graph, "anonymous");
	}

	/**
	 * Constructor for the DataRecordGenerate object
	 * 
	 * @param generateName
	 *            Any name assigned to this generation.
	 */
	public DataRecordGenerate(TransformationGraph graph, String generateName) {
		this.graph = graph;
		this.generateName = generateName;
	}

	/**
	 * Performs any necessary initialization before generate() method is called
	 * 
	 * @param targetMetadata
	 *            Array of metadata objects
	 * @return True if successfull, otherwise False
	 */
	public boolean init(Properties parameters, DataRecordMetadata[] targetRecordsMetadata) throws ComponentNotReadyException {
		this.parameters = parameters;
		this.targetMetadata = targetRecordsMetadata;
		return init();
	}

	/**
	 * Method to be overriden by user. It is called by init(Properties,DataRecordMetadata[],DataRecordMetadata[]) method
	 * when all standard initialization is performed.<br>
	 * This implementation is just skeleton. User should provide its own.
	 * 
	 * @return true if user's initialization was performed successfully
	 */
	public boolean init() throws ComponentNotReadyException {
		return true;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordGenerate#preExecute()
	 */
	public void preExecute() throws ComponentNotReadyException {
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.RecordGenerate#postExecute(org.jetel.graph.TransactionMethod)
	 */
	public void postExecute(TransactionMethod transactionMethod) throws ComponentNotReadyException {
	}
	
	/**
	 * Generates target data records. Derived class should perform this functionality.<br>
	 * 
	 * @param targetRecords
	 *            output data records (an array)
     * @return RecordTransform.ALL -- send the data record(s) to all the output ports<br>
     *         RecordTransform.SKIP -- skip the data record(s)<br>
     *         >= 0 -- send the data record(s) to a specified output port<br>
     *         < -1 -- fatal error / user defined
	 * @see org.jetel.data.DataRecord#copyFieldsByPosition()
	 */
	public abstract int generate(DataRecord[] outputRecords) throws TransformException;

	/**
	 * Returns description of error if one of the methods failed
	 * 
	 * @return Error message
	 * @since April 18, 2002
	 */
	public String getMessage() {
		return errorMessage;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.RecordTransform#signal() In this implementation does nothing.
	 */
	public void signal(Object signalObject) {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.RecordTransform#getSemiResult()
	 */
	public Object getSemiResult() {
		return null;
	}

	/**
	 * Method called at the end of transformation process.  
	 * 
	 * @see org.jetel.component.RecordTransform#finished()
	 */
	public void finished() {

	}

	/**
	 * Returns IConnection object registered with generation graph under specified name (ID).
	 * 
	 * @param id
	 *            ID of IConnection under which it was registered with graph. It translates to ID if graph is loaded
	 *            from XML
	 * @return IConnection object if found, otherwise NULL
	 */
	public final IConnection getConnection(String id) {
		return graph.getConnection(id);
	}

	/**
	 * Returns DataRecordMetadata object registered with generation graph under specified name (ID).
	 * 
	 * @param id
	 *            ID of DataRecordMetadata under which it was registered with graph. It translates to ID if graph is
	 *            loaded from XML
	 * @return DataRecordMetadata object if found, otherwise NULL
	 */
	public final DataRecordMetadata getDataRecordMetadata(String id) {
		return graph.getDataRecordMetadata(id);
	}

	/**
	 * Returns LookupTable object registered with generation graph under specified name (ID).
	 * 
	 * @param id
	 *            ID of LookupTable under which it was registered with graph. It translates to ID if graph is loaded
	 *            from XML
	 * @return LookupTable object if found, otherwise NULL
	 */
	public final LookupTable getLookupTable(String id) {
		return graph.getLookupTable(id);
	}

	/**
	 * Returns Sequence object registered with generation graph under specified name (ID).
	 * 
	 * @param id
	 *            ID of Sequence under which it was registered with graph. It translates to ID if graph is loaded
	 *            from XML
	 * @return Sequence object if found, otherwise NULL
	 */
	public final Sequence getSequence(String id) {
		return graph.getSequence(id);
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.RecordTransform#setGraph(org.jetel.graph.TransformationGraph)
	 */
	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.RecordTransform#getGraph()
	 */
	public TransformationGraph getGraph() {
		return graph;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.RecordTransform#reset()
	 */
	public void reset() {
		errorMessage = null;
	}

}
