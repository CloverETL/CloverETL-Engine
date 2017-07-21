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
package org.jetel.exception;

import org.jetel.logger.SafeLogUtils;

/**
 * This is exception derived from an existing exception. The new exception
 * should has similar as possible characteristics as the former exception.
 * Only error messages of complete exception chain are obfuscated
 * using {@link SafeLogUtils#obfuscateSensitiveInformation(String)}.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5.6.2013
 */
public class ObfuscatingException extends SerializableException {

	private static final long serialVersionUID = -5439348107967100144L;

	public ObfuscatingException(Throwable e) {
		super(e);
	}

	@Override
	protected ObfuscatingException wrapException(Throwable e) {
		return new ObfuscatingException(e);
	}
	
	@Override
	protected String extractMessage(Throwable e) {
		String message = super.extractMessage(e);
		return SafeLogUtils.obfuscateSensitiveInformation(message);
	}
	
}
