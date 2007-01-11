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
package org.jetel.graph;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;

public interface IGraphElement {

    /**
     * XML attribute of every cloverETL element.
     */
    public final static String XML_ID_ATTRIBUTE = "id";

    public final static String XML_TYPE_ATTRIBUTE = "type";

    /**
     * Check the element configuration.<br>
     * This method is called for each graph element before the graph is executed. This method should
     * verify that all required parameters are set and element may be use.
     */
    public abstract ConfigurationStatus checkConfig(ConfigurationStatus status);

    /**
     *  Initialization of Node
     *
     *@exception  ComponentNotReadyException  Error when trying to initialize
     *      Node/Component
     *@since                                  April 2, 2002
     */
    public abstract void init() throws ComponentNotReadyException;

    /**
     * Free all allocated resources.
     */
    public abstract void free();

    public abstract TransformationGraph getGraph();

    public abstract void setGraph(TransformationGraph graph);

    public abstract String getId();

    public abstract String getName();

    public abstract void setName(String name);

    /**
     * @return <b>true</b> if graph element is checked by checkConfig method; else <b>false</b>
     */
    public abstract boolean isChecked();

    /**
     * @return <b>true</b> if graph element is initialized by init() method; else <b>false</b>
     */
    public abstract boolean isInitialized();
}