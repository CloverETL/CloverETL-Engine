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
package org.jetel.hadoop.connection;


/**
 *
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> 
 * &#169; Javlin, a.s &lt;<a href="http://www.javlin.eu">http://www.javlin.eu</a>&gt
 * @since rel-3-4-0-M1
 * @created 17.11.2012
 */
public class HadoopProviderDefinitionException extends HadoopException{

	private static final long serialVersionUID = -2537322125725020318L;

	//TODO add that constructor to superclass and here
//	public HadoopProviderDefinitionException() {
//		super();
//	}

	public HadoopProviderDefinitionException(String message, Throwable cause) {
		super(message, cause);
	}

	public HadoopProviderDefinitionException(String message) {
		super(message);
	}

	//TODO add that constructor to superclass and here
//	public HadoopProviderDefinitionException(Throwable cause) {
//		super(cause);
//	}
}
