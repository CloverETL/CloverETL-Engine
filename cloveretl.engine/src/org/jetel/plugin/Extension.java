/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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
*/
package org.jetel.plugin;

import java.util.Properties;

/**
 * This class represents extension point of engine.
 * @author Martin Zatopek
 *
 */
public class Extension {

    private final String pointId;
    
    private final Properties parameters;

    private final PluginDescriptor plugin;
    
    public Extension(String pointId, Properties parameters, PluginDescriptor plugin) {
        this.pointId = pointId;
        this.parameters = parameters;
        this.plugin = plugin;
    }

    public Properties getParameters() {
        return parameters;
    }

    public String getParameter(String key) {
        return parameters.getProperty(key);
    }
    
    public String getParameter(String key, String defaultValue) {
        return parameters.getProperty(key, defaultValue);
    }

    public PluginDescriptor getPlugin() {
        return plugin;
    }

    public String getPointId() {
        return pointId;
    }

    public String toString() {
        return "Extension point id: " + getPointId() + "\nParameters:\n" + getParameters();
    }
}
