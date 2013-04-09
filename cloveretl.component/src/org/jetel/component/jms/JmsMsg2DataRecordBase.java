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
package org.jetel.component.jms;

import java.util.Properties;

import org.jetel.component.AbstractDataTransform;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Simple partial implementation of JmsMsg2DataRecord interface. Supposed to be extended by full implementations.
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/28/06  
 */
public abstract class JmsMsg2DataRecordBase extends AbstractDataTransform implements JmsMsg2DataRecord {

	protected String errMsg;

	@Override
	public void init(DataRecordMetadata metadata, Properties props) throws ComponentNotReadyException {
		errMsg = null;
	}

	@Override
	public boolean endOfInput() {
		return false;
	}

	protected void setErrorMsg(String errMsg) {
		this.errMsg = errMsg;
	}

	@Override
	public String getErrorMsg() {
		return errMsg;
	}

	/**
	 * @deprecated Use {@link #preExecute()} method.
	 */
	@Deprecated
	@Override
	public void reset() {
		setErrorMsg(null);
	}

}
