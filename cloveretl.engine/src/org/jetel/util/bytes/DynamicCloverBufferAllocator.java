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
package org.jetel.util.bytes;

import java.nio.ByteBuffer;

/**
 * Buffer allocator for {@link DynamicCloverBuffer}.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20 Oct 2011
 * 
 * @see DynamicCloverBuffer
 * @see CloverBufferAllocator
 * @see CloverBuffer
 */
public class DynamicCloverBufferAllocator implements CloverBufferAllocator {

	@Override
	public CloverBuffer allocate(int capacity, boolean direct) {
		ByteBuffer innerBuffer = direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
		return new DynamicCloverBuffer(innerBuffer);
	}

	@Override
	public CloverBuffer allocate(int capacity, int maximumCapacity, boolean direct) {
		ByteBuffer innerBuffer = direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
		return new DynamicCloverBuffer(innerBuffer, maximumCapacity);
	}

	@Override
	public CloverBuffer wrap(ByteBuffer innerBuffer) {
		return new DynamicCloverBuffer(innerBuffer);
	}
	
	@Override
	public CloverBuffer wrap(ByteBuffer innerBuffer, int maximumCapacity) {
		return new DynamicCloverBuffer(innerBuffer, maximumCapacity);
	}

}
