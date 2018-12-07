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
package org.jetel.util;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.regex.Pattern;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

/**
 * Methods return info about Direct memory. If works only on Oracle JDK 1.7 so far.
 * All methods return -1 when it's not possible to obtain correct number.
 * 
 * @author mvarecha (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created Sep 23, 2013
 */
public class DirectMemoryUtils {
	private static Logger log = Logger.getLogger(DirectMemoryUtils.class);

	private static MBeanServer mbeans = ManagementFactory.getPlatformMBeanServer();
    private static ObjectName directPool;
    /** Since the BufferPool is accessible only on some JVMs (Oracle JDK 1.7), the feature will be disabled when any error occurs. */
    private static boolean disabled = false;
    
    static {
        try {
            // Create the name reference for the direct buffer pool’s MBean
            directPool = new ObjectName("java.nio:type=BufferPool,name=direct");
        } catch (Throwable ex) {
        	disabled = true;
        	log.warn("Error creating direct pool ObjectName; Direct memory tracking won't be available");
        	log.info("Error creating direct pool ObjectName", ex);
        }
    }

    public static long getDirectMemoryUsage() {
    	if (disabled)
    		return -1;
        try {
            Long value = (Long)mbeans.getAttribute(directPool, "MemoryUsed");
            return value == null ? -1 : value;
        } catch (Throwable ex) {
        	log.warn("Error retrieving DirectMemory.MemoryUsed; Direct memory tracking won't be available");
        	log.debug("Error retrieving DirectMemory.MemoryUsed", ex);
        	disabled = true;
            return -1;
        }
    }

    public static long getDirectMemoryCount() {
    	if (disabled)
    		return -1;
        try {
            Long value = (Long)mbeans.getAttribute(directPool, "Count");
            return value == null ? -1 : value;
        } catch (Throwable ex) {
        	log.warn("Error retrieving DirectMemory.Count; Direct memory tracking won't be available");
        	log.debug("Error retrieving DirectMemory.Count", ex);
        	disabled = true;
            return -1;
        }
    }

    public static long getDirectMemoryTotalCapacity() {
    	if (disabled)
    		return -1;
        try {
            Long value = (Long)mbeans.getAttribute(directPool, "TotalCapacity");
            return value == null ? -1 : value;
        } catch (Throwable ex) {
        	log.warn("Error retrieving DirectMemory.TotalCapacity; Direct memory tracking won't be available");
        	log.debug("Error retrieving DirectMemory.TotalCapacity", ex);
        	disabled = true;
            return -1;
        }
    }
    
    public static long getDirectMemoryMaxSize() {
    	if (disabled)
    		return -1L;
        try {
        	long value = -1L;
    		java.lang.management.RuntimeMXBean runtimemxBean = ManagementFactory.getRuntimeMXBean();
    		List<String> args = runtimemxBean.getInputArguments();
    		
    		for(String arg: args) {
    			String[] entry = arg.split("=");
    			if(entry.length==2) {
    				if("-XX:MaxDirectMemorySize".equals(entry[0])) {
    					value = parseHumanReadableSize(entry[1]);
    					break;
    				}
    			}
    		}
            return value;
        } catch (Throwable ex) {
        	log.warn("Error retrieving -XX:MaxDirectMemorySize; Max direct memory size won't be available");
        	log.debug("Error retrieving -XX:MaxDirectMemorySize", ex);
        	disabled = true;
            return -1L;
        }
    }
    
	/**
	 * Parses value from human readable string (size+units) 
	 * and convert it to the value in bytes
	 * For null or empty values returns -1
	 * For examples @see com.cloveretl.server.test.utils.UtilsTest.testParseHumanReadableSize()
	 *  
	 * @param in
	 * @return
	 * @throws IllegalArgumentException if format is invalid
	 */
	public static long parseHumanReadableSize(String in) {
		if(in == null || in.isEmpty()) {
			return -1;
		}
		in = in.trim();
		in = in.replaceAll(",",".");
		try { return Long.parseLong(in); } catch (NumberFormatException e) {}
		final java.util.regex.Matcher m = Pattern.compile("([\\d.,]+)\\s*(\\w)").matcher(in);
		if(!m.find()) {
			throw new IllegalArgumentException("Value "+in+" has an unexpected format");
		}
		int scale = 1;
		switch (m.group(2).charAt(0)) {
			case 'G' : scale *= 1024 * 1024 * 1024;
				break;
			case 'M' : scale *= 1024 * 1024;
				break;
			case 'K' : scale *= 1024; 
				break;
			default: throw new IllegalArgumentException("Value "+in+" has an unexpected format");
		}
  		return Math.round(Double.parseDouble(m.group(1)) * scale);
	}
}