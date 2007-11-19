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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import org.jetel.data.Defaults;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.FileUtils;

public class GraphRuntimeParameters {
	
	public static final String PROJECT_DIR_PROPERTY = "PROJECT_DIR";
	
	TransformationGraph graph;
	private boolean firstCallprojectURL = true;
	private URL projectURL;
	private int trackingInterval = Defaults.WatchDog.DEFAULT_WATCHDOG_TRACKING_INTERVAL;
	private String mbeanName;
	private String graphFileURL;
	private boolean textTracking;
	private String tmpDir = System.getProperty("java.io.tmpdir");
	private boolean useJMX = true;
    
	public GraphRuntimeParameters(TransformationGraph graph){
		this.graph=graph;
	}
	
	
	public void setGraph(TransformationGraph graph){
		this.graph=graph;
	}
	
	public TransformationGraph getGraph(){
		return graph;
	}
	
	/**
     * Returns URL from PROJECT_DIR graph property value.
     * It is used as context URL for conversion from relative to absolute path.
     * @return 
     */
    
    public URL getProjectURL() {
        if(firstCallprojectURL) {
            firstCallprojectURL = false;
            String projectURLStr = graph.getGraphProperties().getStringProperty(PROJECT_DIR_PROPERTY);
            
            if(projectURLStr != null) {
                try {
                    projectURL = FileUtils.getFileURL(null, projectURLStr);
                } catch (MalformedURLException e) {
                    graph.getLogger().warn("Home project dir is not in valid URL format - " + projectURLStr);
                }
            }
        }

        return projectURL;
    }

	public int getTrackingInterval() {
		return trackingInterval;
	}

	public void setTrackingInterval(int trackingInterval) {
		this.trackingInterval = trackingInterval;
	}

	public String getGraphFileURL() {
		return graphFileURL;
	}

	public String getGraphFilename() {
		try {
			URL url = FileUtils.getFileURL(getProjectURL(), graphFileURL);
			return new File(url.getPath()).getName();
		} catch (MalformedURLException ex) {
			// do nothing, return null
			return null;
		}
	}
	
	public void setGraphFileURL(String graphFileURL) {
		this.graphFileURL = graphFileURL;
	}

	public String getMbeanName() {
		return mbeanName;
	}

	public void setMbeanName(String mbeanName) {
		this.mbeanName = mbeanName;
	}

	public boolean isTextTracking() {
		return textTracking;
	}

	public void setTextTracking(boolean textTracking) {
		this.textTracking = textTracking;
	}

	public String getTmpDir() {
		return tmpDir;
	}

	public void setTmpDir(String tmpDir) {
		this.tmpDir = tmpDir;
	}

    public boolean isUseJMX() {
        return useJMX;
    }

    public void setUseJMX(boolean useJMX) {
        this.useJMX = useJMX;
    }
	
}
