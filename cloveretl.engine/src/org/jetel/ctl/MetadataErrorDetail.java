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
package org.jetel.ctl;

import org.jetel.ctl.ErrorMessage.Detail;
import org.jetel.ctl.ASTnode.CLVFFieldAccessExpression;

/**
 * {@link ErrorMessage.Detail} used to indicate missing fields.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jul 4, 2012
 */
public class MetadataErrorDetail implements Detail {

	private final CLVFFieldAccessExpression node;
	
	public MetadataErrorDetail(CLVFFieldAccessExpression node) {
		this.node = node;
	}

	public boolean isOutput() {
		return node.isOutput();
	}

	public int getRecordId() {
		return node.getRecordId();
	}
	
	public String getFieldName() {
		return node.getFieldName();
	}

}
