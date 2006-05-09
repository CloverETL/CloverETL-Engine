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
import java.io.*;

/**
 * An interface defining operations expected from OutputPortDirect object
 * This interface is complementary to OutputPort. In order to work properly,
 * initialization/opening of port must be first done through OutputPort interface calls.
 * 
 * @author     D.Pavlis
 * @since    August 13, 2002
 * @see     	OutputPort
 */
public interface OutputPortDirect {

	// Operations
	
	/**
	 * An operation that passes/writes one record through this port.<br>
     * The passed-in object (ByteBuffer) must be ready to be read - i.e.
     * ByteBuffer.position() should be 0 and ByteBuffer.limit() should be
     * pointing to the end of data in the buffer.
	 *
	 * @param  record                   ByteBuffer containing the data to be written/sent
	 * @exception  IOException           If writing failed during method call
	 * @exception  InterruptedException  If thread waiting to be notified was interrupted
	 * @since                            August 13, 2002
	 */
	public void writeRecordDirect(ByteBuffer record) throws IOException, InterruptedException;

}

