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

import org.jetel.ctl.CTLAbstractTransform;
import org.jetel.ctl.CTLEntryPoint;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Base class of all Java transforms generated by CTL-to-Java compiler from CTL transforms in the DataGenerator
 * component.
 *
 * @author Michal Tomcanyi, Javlin a.s. &lt;michal.tomcanyi@javlin.cz&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 11th June 2010
 * @created 27th July 2009
 *
 * @see RecordGenerate
 */
public abstract class CTLRecordGenerate extends CTLAbstractTransform implements RecordGenerate {

	/** Output data records used by the generator, or <code>null</code> if not accessible. */
	private DataRecord[] outputRecords = null;

	public final boolean init(Properties parameters, DataRecordMetadata[] targetMetadata)
			throws ComponentNotReadyException {
		globalScopeInit();

		return initDelegate();
	}

	/**
	 * Called by {@link #init(Properties, DataRecordMetadata[])} to perform user-specific initialization defined
	 * in the CTL transform. The default implementation does nothing, may be overridden by the generated transform
	 * class.
	 *
	 * @throws ComponentNotReadyException if the initialization fails
	 */
	@CTLEntryPoint(name = "init", required = false)
	protected boolean initDelegate() throws ComponentNotReadyException {
		// does nothing and succeeds by default, may be overridden by generated transform classes
		return true;
	}

	@CTLEntryPoint(name = "preExecute", required = false)
	public void preExecute() throws ComponentNotReadyException {
		// does nothing by default, may be overridden by generated transform classes
	}

	public final int generate(DataRecord[] target) throws TransformException {
		int result = 0;

		// only output records are accessible within the generate() function
		outputRecords = target;

		try {
			result = generateDelegate();
		} catch (ComponentNotReadyException exception) {
			// the exception may be thrown by lookups, sequences, etc.
			throw new TransformException("Generated transform class threw an exception!", exception);
		}

		// make the output records inaccessible again
		outputRecords = null;

		return result;
	}

	/**
	 * Called by {@link #generate(DataRecord[])} to generate user-specific data record defined in the CTL transform.
	 * Has to be overridden by the generated transform class.
	 *
	 * @throws ComponentNotReadyException if some internal initialization failed
	 * @throws TransformException if an error occurred
	 */
	@CTLEntryPoint(name = "generate", required = true)
	protected abstract int generateDelegate() throws ComponentNotReadyException, TransformException;

	public final void signal(Object signalObject) {
		// does nothing
	}

	public final Object getSemiResult() {
		return null;
	}

	@CTLEntryPoint(name = "getMessage", required = false)
	public String getMessage() {
		// null by default, may be overridden by generated transform classes
		return null;
	}

	@CTLEntryPoint(name = "postExecute", required = false)
	public void postExecute() throws ComponentNotReadyException {
		// does nothing by default, may be overridden by generated transform classes
	}

	protected final DataRecord getInputRecord(int index) {
		throw new TransformLangExecutorRuntimeException(INPUT_RECORDS_NOT_ACCESSIBLE);
	}

	protected final DataRecord getOutputRecord(int index) {
		if (outputRecords == null) {
			throw new TransformLangExecutorRuntimeException(OUTPUT_RECORDS_NOT_ACCESSIBLE);
		}

		if (index < 0 || index >= outputRecords.length) {
			throw new TransformLangExecutorRuntimeException(new Object[] { index }, OUTPUT_RECORD_NOT_DEFINED);
		}

		return outputRecords[index];
	}

	/**
	 * Use {@link #postExecute()} method.
	 */
	@Deprecated
	public void finished() {
	}

	/**
	 * Use {@link #preExecute()} method.
	 */
	@Deprecated
	public void reset() {
	}

}
