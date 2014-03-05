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

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.jetel.data.GraphElementDescription;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.MetadataRepository;

/**
 * This is description of a component type loaded from 'component' extension point.
 * 
 * @author Martin Zatopek
 */
public interface ComponentDescription extends GraphElementDescription {

	public final static String EXTENSION_POINT_ID = "component";

    /**
     * @return class name of metadata provider or null if no provider has been specified.
     */
    public String getMetadataProvider();

    /**
     * @return deep description which contains details about component, number of ports, attributes, ...
     */
    public Component getDescription();
    
    /**
     * @return deep description is optional (introduced in 3.6)
     */
    public boolean hasDescription();
    
    /**
     * @return true if this component propagates metadata from first input port to all output ports
     * @note identical with getDescription().isPassThrough()
     */
    public boolean isPassThrough();
    
    /**
     * Returns ID of metadata associated with given port.
     * Metadata needs to be defined statically in component description
     * in plugin.xml
     * Metadata ID can be used to get real metadata from {@link MetadataRepository}. 
     */
    public String getDefaultInputMetadataId(int portIndex);

    /**
     * Returns ID of metadata associated with given port.
     * Metadata needs to be defined statically in component description
     * in plugin.xml
     * Metadata ID can be used to get real metadata from {@link MetadataRepository}. 
     */
    public String getDefaultOutputMetadataId(int portIndex);

    @XmlRootElement(name = "ETLComponent")
    public static class Component {
    	private String name;
    	private String type;
		private String className;
		private String metadataProvider;
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
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
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
		public String getMetadataProvider() {
			return metadataProvider;
		}
		public void setMetadataProvider(String metadataProvider) {
			this.metadataProvider = metadataProvider;
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
    	public Port getPort(int portIndex) {
    		for (SinglePort singlePort : singlePorts) {
    			if (singlePort.getIndex() == portIndex) {
    				return singlePort;
    			}
    		}
    		if (!multiPorts.isEmpty()) {
    			return multiPorts.get(0);
    		}
    		return null;
    	}
    }
    
    public static interface Port {
    	public Metadata getMetadata();
    }
    
    @XmlRootElement(name = "singlePort")
    public static class SinglePort implements Port {
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
		public int getIndex() {
			try {
				return Integer.parseInt(name);
			} catch (NumberFormatException e) {
				throw new JetelRuntimeException("Name of single port '" + name + "' is not valid integer.", e);
			}
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
		@Override
		@XmlElement(name = "Metadata")
		public Metadata getMetadata() {
			return metadata;
		}
		public void setMetadata(Metadata metadata) {
			this.metadata = metadata;
		}
    }

    @XmlRootElement(name = "multiplePort")
    public static class MultiPort implements Port {
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
		@Override
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
