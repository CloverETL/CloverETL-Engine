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
package org.jetel.component.hadooploader;

import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

/**
 * Class providing static method for converting properties into Hadoop specific configuration. Acts as a bridge between
 * general java properties API and Hadoop specific configuration API.
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         &lt;<a href="http://www.javlin.eu">http://www.javlin.eu</a>&gt
 * @version $Revision$, $Date$
 * @created 9.11.2012
 */
public class HadoopConfigurationUtil {

	/**
	 * Translates <code>Properties</code> instance into corresponding <code>Configuration</code> instance holding
	 * textual representation of key/value pairs given as argument.
	 * 
	 * @param properties
	 *            Properties to be translated into Hadoop specific configuration. If <code>null</code> empty
	 *            configuration is returned.
	 * @return Configuration holding textual representation of given properties.
	 * 
	 * @see Properties
	 * @see Configuration
	 */
	public static Configuration property2Configuration(Properties properties) {
		Configuration config = new Configuration();
		if (properties != null) {
			for (Entry<?, ?> entry : properties.entrySet()) {
				config.set(entry.getKey().toString(), entry.getValue().toString());
			}
		}
		return config;
	}
}
