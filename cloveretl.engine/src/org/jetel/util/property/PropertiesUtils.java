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
package org.jetel.util.property;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.Enumeration;
import java.util.Properties;

import org.jetel.util.string.StringUtils;

/**
 * Utility methods for {@link Properties} handling.
 * 
 * Dec 13, 2011 moved from cloveretl.commons/src/com/cloveretl/commons/utils/PropertiesUtils.java as needed also in engine. SVN history lost.
 *
 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 * @since Feb 20, 2009
 */
public class PropertiesUtils {
	/**
	 * Loads properties from stream.
	 * 
	 * @param props
	 * @param is
	 *            source for the properties. This method does NOT close the stream.
	 * @throws IOException
	 */
	public static void loadProperties(Properties props, InputStream is) throws IOException {
		// TODO the following Properties.load() does not work for a remote stream,
		// we get a Stream already closed error
		// data.load(is);

		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		char[] charBuf = new char[256];
		StringBuilder sb = new StringBuilder();

		int readNum;
		while ((readNum = br.read(charBuf, 0, charBuf.length)) != -1) {
			sb.append(charBuf, 0, readNum);
		}

		props.load(new ByteArrayInputStream(sb.toString().getBytes()));
	}

	/**
	 * Stores the given properties ignoring those with an empty string as a value.
	 * 
	 * @param properties
	 *            properties to be stored
	 * @param outputStream
	 *            an output stream to be used for storage
	 * 
	 * @throws IOException
	 *             if storing fails for any reason
	 */
	public static void storeProperties(Properties properties, OutputStream outputStream) throws IOException {
		Properties propertiesCopy = new Properties();
		Enumeration<?> propertyNames = properties.propertyNames();

		while (propertyNames.hasMoreElements()) {
			String key = (String) propertyNames.nextElement();
			String value = properties.getProperty(key);

			if (!StringUtils.isEmpty(value)) {
				propertiesCopy.setProperty(key, value);
			}
		}

		propertiesCopy.store(outputStream, null);
	}

	/**
	 * Stores graph properties, storing property with key projectPropertyKey first with special comment.
	 * 
	 * @param properties
	 *            properties to be stored
	 * @param outputStream
	 *            an output stream to be used for storage
	 * @param projectPropertyKey
	 *            key of properties stored first
	 * @param comment
	 *            comment for first property
	 * @param projectSerialPostfix
	 *            text stored after first property (use '\n\n')
	 */
	public static void storeGraphProperties(Properties properties, OutputStream outputStream,
			String projectPropertyKey, String comment, String projectSerialPostfix) {
		try {
			String projectProp = properties.getProperty(projectPropertyKey);
			properties.remove(projectPropertyKey);
			if (projectSerialPostfix == null) {
				projectSerialPostfix = "\n\n";
			}
			if (projectProp != null) {
				Properties tmpProp = new Properties();
				tmpProp.setProperty(projectPropertyKey, projectProp);
				tmpProp.store(outputStream, comment);
				outputStream.write(projectSerialPostfix.getBytes());
			}
			properties.store(outputStream, null);
			if (projectProp != null) {
				properties.setProperty(projectPropertyKey, projectProp);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
