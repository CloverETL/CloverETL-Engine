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

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.jetel.data.GraphElementDescription;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.plugin.Extension;
import org.w3c.dom.NodeList;

/**
 * This is description of a component type loaded from 'component' extension point.
 * 
 * @author Martin Zatopek
 */
public class ComponentDescription extends GraphElementDescription {

    public final static String EXTENSION_POINT_ID = "component";
    
    private Component componentDesc;
    
    public ComponentDescription(Extension componentExtension) {
        super(EXTENSION_POINT_ID, componentExtension);
        
	    NodeList xmlContent = getExtension().getXMLDefinition().getElementsByTagName("ETLComponent");
        if (xmlContent.getLength() == 1) {
        	try {
			    JAXBContext context = JAXBContext.newInstance(Component.class);
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
     * @return deep description which contains details about component, number of ports, attributes, ...
     */
    public Component getDescription() {
    	return componentDesc;
    }
    
    /**
     * @return deep description is optional (introduced in 3.6)
     */
    public boolean hasDescription() {
    	return componentDesc != null;
    }
    
    /**
     * @return true if this component propagates metadata from first input port to all output ports
     * @note identical with getDescription().isPassThrough()
     */
    public boolean isPassThrough() {
    	if (hasDescription()) {
    		return getDescription().isPassThrough();
    	} else {
    		return false;
    	}
    }
    
    @XmlRootElement(name = "ETLComponent")
    public static class Component {
    	private String type;
		private String className;
    	private boolean passThrough = false;
    	@XmlElement(name = "inputPorts")
    	private Ports inputPorts;
    	@XmlElement(name = "outputPorts")
    	private Ports outputPorts;
    	public Ports getInputPorts() {
    		return inputPorts;
    	}
    	public Ports getOutputPorts() {
    		return outputPorts;
    	}
    	@XmlAttribute
		public String getType() {
			return type;
		}
		public void setType(String type) {
			this.type = type;
		}
    	@XmlAttribute
		public String getClassName() {
			return className;
		}
		public void setClassName(String className) {
			this.className = className;
		}
    	@XmlAttribute
		public boolean isPassThrough() {
			return passThrough;
		}
		public void setPassThrough(boolean passThrough) {
			this.passThrough = passThrough;
		}
    }
    
    public static class Ports {
    	@XmlElement(name = "singlePort")
    	private List<SinglePort> singlePorts = new ArrayList<SinglePort>();
    	@XmlElement(name = "multiplePort")
    	private List<MultiPort> multiPorts = new ArrayList<MultiPort>();
    	public List<SinglePort> getSinglePorts() {
    		return singlePorts;
    	}
    	public List<MultiPort> getMultiPorts() {
    		return multiPorts;
    	}
    }
    
    @XmlRootElement(name = "singlePort")
    public static class SinglePort {
    	private String name;
    	private String label;
    	private boolean required = false;
    	private Metadata metadata;
		@XmlAttribute
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
    	@XmlAttribute
		public String getLabel() {
			return label;
		}
		public void setLabel(String label) {
			this.label = label;
		}
    	@XmlAttribute
		public boolean isRequired() {
			return required;
		}
		public void setRequired(boolean required) {
			this.required = required;
		}
		@XmlElement(name = "Metadata")
		public Metadata getMetadata() {
			return metadata;
		}
		public void setMetadata(Metadata metadata) {
			this.metadata = metadata;
		}
    }

    @XmlRootElement(name = "multiplePort")
    public static class MultiPort {
    	private String name;
    	private String label;
    	private boolean required = false;
    	private Metadata metadata;
		@XmlAttribute
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
    	@XmlAttribute
		public String getLabel() {
			return label;
		}
		public void setLabel(String label) {
			this.label = label;
		}
    	@XmlAttribute
		public boolean isRequired() {
			return required;
		}
		public void setRequired(boolean required) {
			this.required = required;
		}
		@XmlElement(name = "Metadata")
		public Metadata getMetadata() {
			return metadata;
		}
		public void setMetadata(Metadata metadata) {
			this.metadata = metadata;
		}
    }

    @XmlRootElement(name = "Metadata")
    public static class Metadata {
    	private String id;
    	@XmlAttribute
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
    }
    
}
