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
package org.jetel.connection;

import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;

import org.jetel.database.sql.DBConnection;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.test.CloverTestCase;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18.10.2013
 */
public class TestDriverFromRuntimeContext extends CloverTestCase {

	public void testDriverFromRuntimeContext() throws Exception {
		
		GraphRuntimeContext ctx = new GraphRuntimeContext();
		ctx.setUseJMX(false);
		
		URL graphUrl = getClass().getResource("/org/jetel/connection/test-driver-from-runtime.grf");
		InputStream in = graphUrl.openStream();
		
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(in, ctx);
		
		in.close();
		
		URL driverUrl = getClass().getResource("/org/jetel/connection/hsqldb.jar");
		
		URLClassLoader loader = new URLClassLoader(new URL[] {driverUrl});
		ctx.setClassLoader(loader);
		
		EngineInitializer.initGraph(graph, ctx);
		
		DBConnection dbConnection = (DBConnection)graph.getConnection("JDBC0");
		
		assertNotNull("JDBC driver not loaded", dbConnection.getJdbcDriver().getDriver());
		
		graph.free();
	}
}
