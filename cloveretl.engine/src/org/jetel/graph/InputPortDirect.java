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

package org.jetel.graph;
import java.nio.*;
import java.io.IOException;

/**
 * An interface defining operations expected from InputPortDirect object
 * This interface is complementary to InputPort. In order to work properly,
 * initialization/opening of port must be first done through InputPort interface calls.
 *
 * @author     D.Pavlis
 * @since    August 13, 2002
 * @see        InputPort
 */
public interface InputPortDirect {

	// Operations
	/**
	 *An operation that reads one record from this port
	 *
	 * @param  record                    Description of Parameter
	 * @return                           True if success, otherwise false (when no more data available)
	 * @exception  IOException           Description of Exception
	 * @exception  InterruptedException  Description of Exception
	 * @since                            April 2, 2002
	 */
	public boolean readRecordDirect(ByteBuffer record) throws IOException, InterruptedException;
	
	/**
	 * Method which tests whether data is awaiting/ready to be read
	 * 
	 * @return True if read operation won't block due to lack of data
	 */
	public boolean hasData();
}

