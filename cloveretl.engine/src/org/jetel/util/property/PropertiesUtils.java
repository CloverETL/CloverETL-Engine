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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Enumeration;
import java.util.Properties;

import org.jetel.exception.JetelRuntimeException;
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
		/* "Stream already closed" error is fixed, see CLO-541
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		char[] charBuf = new char[256];
		StringBuilder sb = new StringBuilder();

		int readNum;
		while ((readNum = br.read(charBuf, 0, charBuf.length)) != -1) {
			sb.append(charBuf, 0, readNum);
		}

		props.load(new ByteArrayInputStream(sb.toString().getBytes()));
		*/
		props.load(is);
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

	/**
	 * Converts the given string to {@link Properties}.
	 * Inverse function for {@link #formatProperties(Properties)}.
	 * @param s string representation of properties
	 * @return properties parsed from the given string
	 */
	public static Properties parseProperties(String s) {
		if (s == null) {
			return null;
		} else {
			Properties result = new Properties();
			try {
				result.load(new StringReader(s));
			} catch (IOException e) {
				throw new JetelRuntimeException("properties cannot be parsed from string '" + s + "'");
			}
			return result;
		}
	}
	
	/**
	 * Formats the given properties to string.
	 * Inverse function for {@link #parseProperties(String)}.
	 * @param p properties to be converted to string
	 * @return string representation of the given properties
	 */
	public static String formatProperties(Properties p) {
		if (p == null) {
			return null;
		}
		StringWriter result = new StringWriter();
		try {
			p.store(result, null);
		} catch (IOException e) {
			throw new JetelRuntimeException("properties cannot be formatted to string '" + p + "'");
		}
		return removeFirstLine(result.toString());
	}

	private static final String LINE_SEPARATOR = System.getProperties().getProperty("line.separator");

	private static String removeFirstLine(String s) {
		if (s == null) {
			return null;
		}
		if (s.indexOf(LINE_SEPARATOR) != -1) {
			return s.substring(s.indexOf(LINE_SEPARATOR) + LINE_SEPARATOR.length());
		} else {
			return s;
		}
	}

}
