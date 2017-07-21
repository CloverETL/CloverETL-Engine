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
package org.jetel.data.formatter.provider;

import org.jetel.data.formatter.Formatter;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Dec 13, 2012
 */
public interface SharedFormatterProvider extends FormatterProvider {
	
	/**
	 * Creates a new data formatter. 
	 * The formatter may share some its parts with other formatters,
	 * hence at most one such formatter may be used at a time.
	 * The formatters should also use the same metadata.
	 * 
	 * Used e.g. for partitioning in writers - all partitions
	 * have the same metadata and at most one is being written to at a time.
	 * That allows the DataFormatter.fieldBuffer to be shared.
	 * 
	 * @return data formatter
	 */
	public Formatter getNewSharedFormatter(DataRecordMetadata metadata);

}
