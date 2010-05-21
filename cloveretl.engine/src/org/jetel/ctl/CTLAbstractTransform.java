/*
 * jETeL/Clover - Java based ETL application framework.
 * Copyright (c) Opensys TM by Javlin, a.s. (www.opensys.com)
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */
package org.jetel.ctl;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.dictionary.StringDictionaryType;

/**
 * Base class of all CTL transform classes.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 21st May 2010
 * @created 5th May 2010
*/
public abstract class CTLAbstractTransform {

	/** An empty array of data records used for calls to functions that do not access any data records. */
    protected static final DataRecord[] NO_DATA_RECORDS = new DataRecord[0];

    /** A transformation graph associated with this transform used to query LUTs, sequences, etc. */
	protected TransformationGraph graph;

	/** Input data records initialized and used by specific CTL transformations that extend this class. */
	protected DataRecord[] inputRecords;
	/** Output data records initialized and used by specific CTL transformations that extend this class. */
	protected DataRecord[] outputRecords;

    /**
     * Associates a graph with this CTL transform.
     *
     * @param graph a <code>TransformationGraph</code> graph to be set
     */
	public final void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

    /**
	 * @return a <code>TransformationGraph</code> associated with this CTL transform, or <code>null</code>
	 * if no graph is associated
	 */
	public final TransformationGraph getGraph() {
		return graph;
	}

	/**
	 * Initializes the global scope of the transform. Has to be overridden by the generated transform class.
	 */
	@CTLEntryPoint(name = "globalScopeInit", required = true)
	public abstract void globalScopeInit() throws ComponentNotReadyException;

	/**
	 * Inserts value into Dictionary under specified key. if put fails transforms checked exception into runtime one.
	 * 
	 * @param key
	 * @param value
	 */
	protected final void writeDict(String key, String value) {
		try {
			graph.getDictionary().setValue(key, StringDictionaryType.TYPE_ID, value);
		} catch (ComponentNotReadyException e) {
			throw new TransformLangExecutorRuntimeException(e.getMessage());
		}
	}

	// TODO: All common methods should be moved here when there is a common transformation interface.

}
