/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-2007  David Pavlis <david.pavlis@centrum.cz> and others.
 *    
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *    
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
 *    Lesser General Public License for more details.
 *    
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Created on 10.7.2007 by dadik
 *
 */

package org.jetel.graph.runtime;

import java.util.Properties;

import org.jetel.data.Defaults;

/**
 * Common used implementation of IGraphRuntimeContext interface.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 27.11.2007
 */
public class GraphRuntimeContext implements IGraphRuntimeContext {
	
	private int trackingInterval = Defaults.WatchDog.DEFAULT_WATCHDOG_TRACKING_INTERVAL;
	private boolean useJMX = true;
	private boolean verboseMode = false;
	private Properties additionalProperties = new Properties();
	private boolean checkConfig = true;

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#createCopy()
	 */
	public IGraphRuntimeContext createCopy() {
		GraphRuntimeContext ret = new GraphRuntimeContext();
		
		ret.additionalProperties = new Properties();
		ret.additionalProperties.putAll(getAdditionalProperties());
		ret.trackingInterval = getTrackingInterval();
		ret.checkConfig = isCheckConfig();
		ret.verboseMode = isVerboseMode();
		ret.useJMX = useJMX();
		
		return ret;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#getTrackingInterval()
	 */
	public int getTrackingInterval() {
		return trackingInterval;
	}

	/**
	 * Sets tracking interval for WatchDog's JMX.
	 * @param trackingInterval
	 */
	public void setTrackingInterval(int trackingInterval) {
		this.trackingInterval = trackingInterval;
	}

//	public String getGraphName() {
//		try {
//			URL url = FileUtils.getFileURL(getProjectURL(), graphFileURL);
//			return new File(url.getPath()).getName();
//		} catch (MalformedURLException ex) {
//			// do nothing, return null
//			return null;
//		}
//	}
	
    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.IGraphRuntimeContext#useJMX()
     */
    public boolean useJMX() {
        return useJMX;
    }

    /**
     * Sets whether JMX should be used during graph processing.
     * @param useJMX
     */
    public void setUseJMX(boolean useJMX) {
        this.useJMX = useJMX;
    }

    /**
     * Sets whether should be done only checkConfiguration().
     * @param checkConfig
     */
    public void setCheckConfig(boolean checkConfig) {
    	this.checkConfig = checkConfig;
    }

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#isCheckConfig()
	 */
	public boolean isCheckConfig() {
		return checkConfig;
	}

	/**
	 * Sets whether graph will be processed in so called "verbose mode".
	 * @param verboseMode
	 */
	public void setVerboseMode(boolean verboseMode) {
		this.verboseMode = verboseMode;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#isVerboseMode()
	 */
	public boolean isVerboseMode() {
		return verboseMode;
	}

	/**
	 * Adds new additional property (key-value pair).
	 * @param key
	 * @param value
	 */
	public void addAdditionalProperty(String key, String value) {
		additionalProperties.setProperty(key, value);
	}

	/**
	 * Adds new collection of additional properties.
	 * @param properties
	 */
	public void addAdditionalProperties(Properties properties) {
		additionalProperties.putAll(properties);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#getAdditionalProperties()
	 */
	public Properties getAdditionalProperties() {
		return additionalProperties;
	}

}
