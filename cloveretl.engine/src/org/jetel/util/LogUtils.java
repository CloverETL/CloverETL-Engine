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
import java.lang.management.MemoryMXBean;
import java.text.MessageFormat;

import javax.tools.ToolProvider;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

/**
 * Logging utilities for engine.
 * 
 * @author Kokon, salamonp (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12 Apr 2012
 */
public class LogUtils {

	public static final String MDC_RUNID_KEY = "runId";
	
	private static final MemoryMXBean memMXB = ManagementFactory.getMemoryMXBean();
	
	private static final Logger logger = Logger.getLogger(LogUtils.class);
	
	private static Boolean isCompilerAvailable;
	
	private static MessageFormat RUNTIME_HEADER_1 = new MessageFormat("***  CloverDX, (c) 2002-{0} Javlin a.s.  ***");
	private static MessageFormat RUNTIME_HEADER_2 = new MessageFormat("Running with {0}");
	private static MessageFormat RUNTIME_HEADER_3 = new MessageFormat(
			"Running on {0} CPU(s)" +
			", OS {1}" +
			", architecture {2}");
	private static MessageFormat RUNTIME_HEADER_4 = new MessageFormat("Running on {0}, {1}, {2} ({3}), max available memory for JVM: {4} MB");
	
	/**
	 * Uses engine Logger.
	 */
	public static void printRuntimeHeader() {
		printRuntimeHeader(logger);
	}
	
	public static void printRuntimeHeader(Logger logger) {
		logger.info(RUNTIME_HEADER_1.format(new Object[] {JetelVersion.LIBRARY_BUILD_YEAR}));
		logger.info(RUNTIME_HEADER_2.format(new Object[] {JetelVersion.getProductName()}));
		logger.info(RUNTIME_HEADER_3.format(new Object[] {
				Runtime.getRuntime().availableProcessors(),
				System.getProperty("os.name"),
				System.getProperty("os.arch")
		}));
	}
	
	/**
	 * Uses engine Logger.
	 */
	public static void printJvmInfo() {
		printJvmInfo(logger);
	}
	
	public static void printJvmInfo(Logger logger) {
		logger.info(RUNTIME_HEADER_4.format(new Object[] {
				System.getProperty("java.runtime.name"),
				System.getProperty("java.version"),
				System.getProperty("java.vendor"),
				(isCompilerAvailable() ? "JDK" : "JRE - runtime compilation is not available!"),
				Long.toString(memMXB.getHeapMemoryUsage().getMax() / (1024 * 1024))
		}));
	}
	
	/**
	 * @return true if JDK with valid java compiler is detected, false for simple JRE
	 */
	private static synchronized boolean isCompilerAvailable() {
		if (isCompilerAvailable == null) {
			isCompilerAvailable = ToolProvider.getSystemJavaCompiler() != null;
		}
		return isCompilerAvailable;
	}
	
	/**
	 * Executes the Runnable with the MDC context set to <code>runId</code> and restores the original context afterwards.
	 * 
	 * @param runId		context job runId
	 * @param runnable
	 */
	public static void runWithRunIdContext(long runId, Runnable runnable) {
		Object oldRunId = MDC.get(LogUtils.MDC_RUNID_KEY);
		MDC.put(LogUtils.MDC_RUNID_KEY, runId);
		try {
			runnable.run();
		} finally {
			if (oldRunId == null) {
				MDC.remove(LogUtils.MDC_RUNID_KEY);
			} else {
				MDC.put(LogUtils.MDC_RUNID_KEY, oldRunId);
			}
		}
	}

}
