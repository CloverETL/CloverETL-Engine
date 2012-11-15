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

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

/**
 * 
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         &lt;<a href="http://www.javlin.eu">http://www.javlin.eu</a>&gt
 * @version $Revision$, $Date$
 * @created 9.11.2012
 */
public interface IHadoopMapReduceJobSender {

	public boolean connect(URI jobTrackerHost, URI nameNodeHost) throws IOException;
	public boolean connect(URI jobTrackerHost, URI nameNodeHost, Properties properties) throws IOException;
	public String validateConnection() throws IOException;
	public void close() throws IOException;
	
	public void sendJob(HadoopMapReduceJobDetails job) throws IOException;
}
