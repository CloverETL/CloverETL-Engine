/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
 */

package org.jetel.test;

import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;



/**
 * Rule for disable logging.
 * 
 * Copied from com.cloveretl.server.worker.commons.SuspendLoggingRule
 * to avoid the need of changing the build system, engine tests are not on server test classpath.
 * 
 * @author kocik (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jun 26, 2018
 */
public class SuspendLoggingRule extends TestWatcher {

	/**
	 * Names of disabled methods. If it is null then loggining in every method is off.
	 */
	private final List<String> methodNames;
	private Level originalLevel;
	private Logger logger;

	public SuspendLoggingRule(Class<?> clazz) {
		this(clazz, null);
	}
	
	public SuspendLoggingRule(Class<?> clazz, List<String> methodNames) {
		this.methodNames = methodNames;
		
		logger = Logger.getLogger(clazz);
		originalLevel = logger.getLevel();
	}

	@Override
	public void starting(Description desc) {
		if (methodNames == null || methodNames.contains(desc.getMethodName())) {
			logger.setLevel(Level.OFF);
		}
	}

	@Override
	public void finished(Description desc) {
		logger.setLevel(originalLevel);
	}
}
