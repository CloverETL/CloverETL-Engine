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
package org.jetel.ctl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.Transform;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.dictionary.StringDictionaryType;

/**
 * Base class of all CTL transform classes.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 17th June 2010
 * @created 5th May 2010
 */
public abstract class CTLAbstractTransform implements Transform {

	/** The logger used by this class and generated transform classes. */
	protected static final Log logger = LogFactory.getLog(CTLAbstractTransform.class);

	/** Runtime error message used when input records are not accessible. */
	protected static final String INPUT_RECORDS_NOT_ACCESSIBLE = "Cannot access input records within this scope!";
	/** Runtime error message used when invalid input record is requested. */
	protected static final String INPUT_RECORD_NOT_DEFINED = "No input record defined for given index!";

	/** Runtime error message used when output records are not accessible. */
	protected static final String OUTPUT_RECORDS_NOT_ACCESSIBLE = "Cannot access output records within this scope!";
	/** Runtime error message used when invalid output record is requested. */
	protected static final String OUTPUT_RECORD_NOT_DEFINED = "No output record defined for given index!";

	/** A graph node associated with this CTL transform used to query graph, LUTs, sequences, etc.. */
	private Node node;

	@Override
	public void setNode(Node node) {
		this.node = node;
	}

	@Override
	public Node getNode() {
		return node;
	}

	@Override
	public final TransformationGraph getGraph() {
		return (node != null) ? node.getGraph() : null;
	}

	/**
	 * Initializes the global scope of the transform. Has to be overridden by the generated transform class.
	 */
	@CTLEntryPoint(name = "globalScopeInit", required = true)
	public abstract void globalScopeInit() throws ComponentNotReadyException;

	@Override
	@CTLEntryPoint(name = "preExecute", required = false)
	public void preExecute() throws ComponentNotReadyException {
		// does nothing by default, may be overridden by generated transform classes
	}

	@Override
	@CTLEntryPoint(name = "postExecute", required = false)
	public void postExecute() throws ComponentNotReadyException {
		// does nothing by default, may be overridden by generated transform classes
	}

	@Override
	@CTLEntryPoint(name = "getMessage", required = false)
	public String getMessage() {
		// null by default, may be overridden by generated transform classes
		return null;
	}

	/**
	 * @deprecated Use {@link #postExecute()} method.
	 */
	@Deprecated
	@Override
	@CTLEntryPoint(name = "finished", required = false,
			deprecated = "Call to the deprecated finished() function ignored, use postExecute() instead!")
	public void finished() {
		// does nothing by default, may be overridden by generated transform classes
	}

	/**
	 * @deprecated Use {@link #preExecute()} method.
	 */
	@Deprecated
	@Override
	@CTLEntryPoint(name = "reset", required = false,
			deprecated = "Call to the deprecated reset() function ignored, use preExecute() instead!")
	public void reset() {
		// does nothing by default, may be overridden by generated transform classes
	}

	/**
	 * Returns a data record for a given input port index.
	 *
	 * @param index an input port index
	 *
	 * @return a data record for a given input port index
	 *
	 * @throws TransformLangExecutorRuntimeException if input records are not accessible,
	 * or if an invalid index was provided
	 */
	protected abstract DataRecord getInputRecord(int index);

	/**
	 * Returns a data record for a given output port index.
	 *
	 * @param index an output port index
	 *
	 * @return a data record for a given output port index
	 *
	 * @throws TransformLangExecutorRuntimeException if output records are not accessible,
	 * or if an invalid index was provided
	 */
	protected abstract DataRecord getOutputRecord(int index);

	/**
	 * Inserts value into Dictionary under specified key. if put fails transforms checked exception into runtime one.
	 * 
	 * @param key
	 * @param value
	 */
	protected final void writeDict(String key, String value) {
		try {
			getGraph().getDictionary().setValue(key, StringDictionaryType.TYPE_ID, value);
		} catch (ComponentNotReadyException e) {
			throw new TransformLangExecutorRuntimeException("Dictionary writing failed", e);
		}
	}

}
