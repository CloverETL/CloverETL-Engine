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
package org.jetel.util.exec;

import java.io.IOException;
import java.io.OutputStream;

import org.jetel.exception.JetelException;

/**
 * Interface for instances able to produce process' input.
 * @see org.jetel.util.exec.ProcBox
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 10/11/06 
 */
public interface DataProducer {
	/**
	 * Set stream to be supplied with produced data. It is supposed to be called exactly once and before any call
	 * to other interface's methods.
	 * @param stream
	 */
	public void setOutput(OutputStream stream);
	/**
	 * Produces one piece of data.
	 * @return false when no more data are available, true otherwise.
	 * @throws JetelException
	 */
	public boolean produce() throws JetelException;
	/**
	 * This is supposed to release resources used by other methods.
	 * It should not close output stream!
	 */
	public void close() throws IOException;
}
