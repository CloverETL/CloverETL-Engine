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
package org.jetel.connection.nosql;

import java.sql.SQLException;
import java.util.Properties;

import org.jetel.database.IConnection;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Mar 19, 2013
 */
public abstract class AbstractNoSQLConnection extends GraphElement implements IConnection {

	public static final String XML_CONFIG_FILE_ATTRIBUTE = "config";
	
	public AbstractNoSQLConnection(String id, String name) {
		super(id, name);
	}

	public AbstractNoSQLConnection(String id, TransformationGraph graph, String name) {
		super(id, graph, name);
	}

	public AbstractNoSQLConnection(String id, TransformationGraph graph) {
		super(id, graph);
	}

	public AbstractNoSQLConnection(String id) {
		super(id);
	}

	@Override
	public DataRecordMetadata createMetadata(Properties parameters) throws SQLException {
		return null;
	}

}
