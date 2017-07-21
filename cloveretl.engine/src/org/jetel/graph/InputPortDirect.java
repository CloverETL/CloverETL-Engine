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
package org.jetel.graph;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.jetel.util.CloverPublicAPI;
import org.jetel.util.bytes.CloverBuffer;

/**
 * An interface defining operations expected from InputPortDirect object
 * This interface is complementary to InputPort. In order to work properly,
 * initialization/opening of port must be first done through InputPort interface calls.
 *
 * @author     D.Pavlis
 * @since    August 13, 2002
 * @see        InputPort
 */
@CloverPublicAPI
public interface InputPortDirect extends InputPort {

	/**
	 * An operation that reads one record from this port - in its serialized (binary form).<br>
     * The passed-in object (content) is cleared first (using CloverBuffer.clear()). When data is stored in
     * it, CloverBuffer.flip() operation is called upon it - it is ready to be read when
     * method call ends.
	 *
	 * @param  record                    CloverBuffer into which data should be stored
	 * @return                           True if success, otherwise false (when no more data available)
	 * @exception  IOException           If reading failed during method call
	 * @exception  InterruptedException  If thread waiting to be notified was interrupted
	 * @since                            April 2, 2002
	 */
	public boolean readRecordDirect(CloverBuffer record) throws IOException, InterruptedException;

	// Operations
	/**
	 * An operation that reads one record from this port - in its serialized (binary form).<br>
     * The passed-in object (content) is cleared first (using ByteBuffer.clear()). When data is stored in
     * it, ByteBuffer.flip() operation is called upon it - it is ready to be read when
     * method call ends.
	 *
	 * @param  record                    ByteBuffer into which data should be stored
	 * @return                           True if success, otherwise false (when no more data available)
	 * @exception  IOException           If reading failed during method call
	 * @exception  InterruptedException  If thread waiting to be notified was interrupted
	 * @since                            April 2, 2002
	 * @deprecated use {@link #readRecordDirect(CloverBuffer)} instead
	 */
	@Deprecated
	public boolean readRecordDirect(ByteBuffer record) throws IOException, InterruptedException;

}
