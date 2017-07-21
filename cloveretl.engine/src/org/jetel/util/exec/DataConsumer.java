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
import java.io.InputStream;

import org.jetel.exception.JetelException;

/**
 * Interface for instances able to consume process' output and/or error output.
 * @see org.jetel.util.exec.ProcBox
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 10/11/06 
 */
public interface DataConsumer {
	/** 
	 * Set stream supplying data to be consumed. It is supposed to be called exactly once and before any call
	 * to other interface's methods.
	 * @param stream
	 */
	public void setInput(InputStream stream);
	/**
	 * Consumes one piece of data.
	 * @return false when no more data are available, true otherwise.
	 * @throws JetelException
	 */
	public boolean consume() throws JetelException;
	/**
	 * This is supposed to release resources used by other methods.
	 * It should not close input stream!
	 */
	public void close() throws IOException;
}
