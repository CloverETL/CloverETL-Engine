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
package org.jetel.data.formatter;

import java.io.IOException;

import org.jetel.util.bytes.CloverBuffer;

/**
 * Basic abstract implementation of {@link Formatter} interface.
 *  
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 16 Mar 2012
 */
public abstract class AbstractFormatter implements Formatter {

	protected boolean append;
	protected boolean appendTargetNotEmpty;
	
	@Override
	public void setAppend(boolean append) {
		this.append = append;
	}
	
	@Override
	public DataTargetType getPreferredDataTargetType() {
		return DataTargetType.CHANNEL;
	}
	
	@Override
	public void setAppendTargetNotEmpty(boolean b) {
		this.appendTargetNotEmpty = b;
	}

	@Override
	public int writeDirect(CloverBuffer record) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isDirect() {
		return false;
	}
	
}
