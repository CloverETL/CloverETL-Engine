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
package org.jetel.component;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.jetel.data.GraphElementDescriptionImpl;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.MetadataRepository;
import org.jetel.plugin.Extension;
import org.jetel.plugin.PluginDescriptor;
import org.jetel.util.JAXBContextProvider;
import org.w3c.dom.NodeList;

/**
 * This is description of a component type loaded from 'component' extension point.
 * 
 * @author Martin Zatopek
 */
public class ComponentDescriptionImpl extends GraphElementDescriptionImpl implements ComponentDescription {

    private Component componentDesc;
    
    public ComponentDescriptionImpl(Extension componentExtension) {
        super(EXTENSION_POINT_ID, componentExtension);
        
	    NodeList xmlContent = getExtension().getXMLDefinition().getElementsByTagName("ETLComponent");
        if (xmlContent.getLength() == 1) {
        	try {
			    JAXBContext context = JAXBContextProvider.getInstance().getContext(Component.class);
			    Unmarshaller m = context.createUnmarshaller();
			    
			    componentDesc = (Component) m.unmarshal(xmlContent.item(0));
        	} catch (JAXBException e) {
        		throw new JetelRuntimeException("Invalid component descrition in plugin " + getPluginDescriptor(), e);
        	}
        }
    }

    @Override
    public String getType() {
    	if (hasDescription()) {
    		return componentDesc.getType();
    	} else {
    		return super.getType();
    	}
    }
    
    @Override
    public String getClassName() {
    	if (hasDescription()) {
    		return componentDesc.getClassName();
    	} else {
    		return super.getClassName();
    	}
    }

    /**
     * @return class name of metadata provider or null if no provider has been specified.
     */
    @Override
	public String getMetadataProvider() {
    	if (hasDescription()) {
    		return componentDesc.getMetadataProvider();
    	} else {
    		return null;
    	}
    }

    /**
     * @return deep description which contains details about component, number of ports, attributes, ...
     */
    @Override
	public Component getDescription() {
    	return componentDesc;
    }
    
    /**
     * @return deep description is optional (introduced in 3.6)
     */
    @Override
	public boolean hasDescription() {
    	return componentDesc != null;
    }
    
    /**
     * @return true if this component propagates metadata from first input port to all output ports
     * @note identical with getDescription().isPassThrough()
     */
    @Override
	public boolean isPassThrough() {
    	if (hasDescription()) {
    		return getDescription().isPassThrough();
    	} else {
    		return false;
    	}
    }
    
    /**
     * Returns ID of metadata associated with given port.
     * Metadata needs to be defined statically in component description
     * in plugin.xml
     * Metadata ID can be used to get real metadata from {@link MetadataRepository}. 
     */
    @Override
	public String getDefaultInputMetadataId(int portIndex) {
    	if (hasDescription()) {
	    	Ports inputPorts = componentDesc.getInputPorts();
	    	Port port = inputPorts.getPort(portIndex);
	    	if (port != null && port.getMetadata() != null) {
	    		return port.getMetadata().getId();
	    	} else {
	    		return null;
	    	}
    	} else {
    		return null;
    	}
    }

    /**
     * Returns ID of metadata associated with given port.
     * Metadata needs to be defined statically in component description
     * in plugin.xml
     * Metadata ID can be used to get real metadata from {@link MetadataRepository}. 
     */
    @Override
	public String getDefaultOutputMetadataId(int portIndex) {
    	if (hasDescription()) {
	    	Ports outputPorts = componentDesc.getOutputPorts();
	    	Port port = outputPorts.getPort(portIndex);
	    	if (port != null && port.getMetadata() != null) {
	    		return port.getMetadata().getId();
	    	} else {
	    		return null;
	    	}
    	} else {
    		return null;
    	}
    }

    public static class MissingComponentDescription implements ComponentDescription {
		@Override
		public String getClassName() {
			return "!UNKNOWN_CLASS_NAME!";
		}

		@Override
		public String getType() {
			return "!UNKNOWN_TYPE!";
		}

		@Override
		public PluginDescriptor getPluginDescriptor() {
			return null;
		}

		@Override
		public void init() throws ComponentNotReadyException {
		}

		@Override
		public Extension getExtension() {
			return null;
		}

		@Override
		public String getMetadataProvider() {
			return null;
		}

		@Override
		public Component getDescription() {
			return null;
		}

		@Override
		public boolean hasDescription() {
			return false;
		}

		@Override
		public boolean isPassThrough() {
			return false;
		}

		@Override
		public String getDefaultInputMetadataId(int portIndex) {
			return null;
		}

		@Override
		public String getDefaultOutputMetadataId(int portIndex) {
			return null;
		}
    }
    
}
