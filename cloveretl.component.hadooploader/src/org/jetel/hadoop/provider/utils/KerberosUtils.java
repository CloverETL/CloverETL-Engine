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
package org.jetel.hadoop.provider.utils;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.jetel.graph.ContextProvider;
import org.jetel.hadoop.provider.HadoopConfigurationUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.string.StringUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12. 10. 2017
 */
public class KerberosUtils {

	/**
	 * 
	 */
	private static final String CLOVERETL_HADOOP_KERBEROS_KEYTAB = "cloveretl.hadoop.kerberos.keytab";

	private static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
	
	public static UserGroupInformation getUserGroupInformation(String user, Configuration config) throws IOException {
		if (isKerberosAuthentication(config)) {
			String keytab = config.get(CLOVERETL_HADOOP_KERBEROS_KEYTAB, "");
			if (!StringUtils.isEmpty(user) && !StringUtils.isEmpty(keytab)) {
				File file = FileUtils.getJavaFile(ContextProvider.getContextURL(), keytab); // FIXME get rid of ContextProvider
				if ((file != null) && file.exists()) {
					keytab = file.getAbsolutePath();
					synchronized (UserGroupInformation.class) { // make sure no other thread changes the static configuration
						UserGroupInformation.setConfiguration(config);
						return UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, keytab);
					}
				}
			}
		}
		
		return null;
	}

	public static boolean isKerberosAuthentication(Configuration config) {
		String authentication = config.get(HADOOP_SECURITY_AUTHENTICATION, "");
		return authentication.equalsIgnoreCase("Kerberos");
	}
	
	/**
	 * Helper method to invoke the specified action with Kerberos principal from the configuration.
	 * If Kerberos authentication is not enabled, just invokes the action and returns the result.
	 * 
	 * @param action	the action
	 * @param user		principal name
	 * @param config	Hadoop configuration properties
	 * 
	 * @return result of the action
	 * @throws Exception
	 */
	public static <T> T doAs(PrivilegedExceptionAction<T> action, String user, Configuration config) throws Exception {
		UserGroupInformation ugi = getUserGroupInformation(user, config);
		if (ugi != null) {
			return ugi.doAs(action);
		} else {
			return action.run();
		}
	}

	/**
	 * Helper method for HiveSpecific.
	 * 
	 * @param action
	 * @param user
	 * @param kerberosProperties
	 * @return
	 * @throws Exception
	 */
	public static <T> T doAs(PrivilegedExceptionAction<T> action, String user, Properties kerberosProperties) throws Exception {
		Configuration config = HadoopConfigurationUtils.property2Configuration(kerberosProperties);
		return doAs(action, user, config);
	}

}
