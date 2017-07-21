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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.graph.dictionary.ReadableChannelDictionaryType;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.main.runGraph;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.exec.PortDataProducer;
import org.jetel.util.exec.ProducerConsumerExecutor;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * <h3>SubGraph Execute Component</h3>
 * <b>
 * IMPORTANT: not tested, write by Cyril for obsolute Subgraph implemantation via Dictionary
 * </b>
 * <table border="1">
 * <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td><td>SubGraph Execute</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td><td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td><td>This component executes another clover graph.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td><td>[0..n]</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td><td>[0..n]</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td><td>In graph design phase graph isn't parsed, graph developer is responsible for match of dictionary keys and metadata.</td></tr>
 * </table>
 * <br>
 * <table border="1">
 * <th>XML attributes:</th>
 * <tr><td><b>type</b></td><td>"SUB_GRAPH_EXECUTE"</td></tr>
 * <tr><td><b>id</b></td><td>component identification</td></tr> 
 * <tr><td><b>graphName</b></td><td>path to XML graph definition, which should be ran</td></tr>
 * </table>
 * 
 */
public class SubGraph extends Node {

	public final static String COMPONENT_TYPE = "SUB_GRAPH_EXECUTE";

	private static final String XML_GRAPH_NAME_ATTRIBUTE = "graphName";
	private static final String XML_INPUT_MAPPING_ATTRIBUTE = "inputMapping";
	private static final String XML_OUTPUT_MAPPING_ATTRIBUTE = "outputMapping";

	private static final Pattern INPUT_MAPPING_PATTERN = Pattern.compile("\\s*dict\\.(\\w+)\\s*:=\\s*dataRecordWriter\\s*\\(\\s*port\\.(\\d+)\\s*\\)\\s*");
	private static final Pattern OUTPUT_MAPPING_PATTERN = Pattern.compile("\\s*port\\.(\\d+)\\s*:=\\s*dataRecordReader\\s*\\(\\s*dict\\.(\\w+)\\s*\\)\\s*");

	private static final String XML_ADDITIONAL_PROPERTIES_ATTRIBUTE = "additionalProperties";

	private String graphName;

	private TransformationGraph graph;
	private Properties additionalProperties;
	private List<PortMapping> inputMapping;
	private List<PortMapping> outputMapping;

	/**
	 * Port routing information.
	 */
	private static class PortMapping {
		private int portNumber;
		private String dictionaryKey;

		public PortMapping(int portNumber, String dictionaryKey) {
			super();
			this.dictionaryKey = dictionaryKey;
			this.portNumber = portNumber;
		}

		public int getPortNumber() {
			return portNumber;
		}

		public void setPortNumber(int portNumber) {
			this.portNumber = portNumber;
		}

		public String getDictionaryKey() {
			return dictionaryKey;
		}

		public void setDictionaryKey(String dictionaryKey) {
			this.dictionaryKey = dictionaryKey;
		}
	}

	public SubGraph(String id, String graphName) {
		super(id);
		this.graphName = graphName;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized())
			return;
		super.init();

		// prepare input stream with XML graph definition
		InputStream in = null;

		try {
			final TransformationGraph parentGraph = getGraph();
			in = Channels.newInputStream(FileUtils.getReadableChannel(parentGraph != null ? parentGraph.getRuntimeContext().getContextURL() : null, graphName));
		} catch (IOException e) {
			throw new ComponentNotReadyException(this, "Embedded graph file '" + graphName + "' cannot be found.", e);
		}

		// create transformation graph instance
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.addAdditionalProperties(additionalProperties);
		try {
			graph = TransformationGraphXMLReaderWriter.loadGraph(in, runtimeContext);
		} catch (Exception e) {
			throw new ComponentNotReadyException(this, "Embedded graph cannot be instantiated.", e);
		}

	}

	@Override
	public Result execute() throws Exception {
		// prepare dictionary of embedded graph
		final Dictionary dictionary = graph.getDictionary();

		EngineInitializer.initGraph(graph);
		Future<Result> futureResult = runGraph.executeGraph(graph, graph.getRuntimeContext());

		final ProducerConsumerExecutor executor = new ProducerConsumerExecutor();

		for (PortMapping mapping : inputMapping) {
			final InputPort port = getInputPort(mapping.getPortNumber());
			final PortDataProducer producer = new PortDataProducer(port);
			final OutputStream dictOutputStream = new FileOutputStream("");
//TODO			
			dictionary.setValue(mapping.getDictionaryKey(), ReadableChannelDictionaryType.TYPE_ID, dictOutputStream);
			executor.addProducer(producer, dictOutputStream);
		}

		broadcastEOF();
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
	}

	/**
	 * Creates this component based on information in the given xml element.
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		try {
			final SubGraph ret = new SubGraph(xattribs.getString(XML_ID_ATTRIBUTE), xattribs.getString(XML_GRAPH_NAME_ATTRIBUTE));
			ret.setInputMapping(importMappingFromString(xattribs.getString(XML_INPUT_MAPPING_ATTRIBUTE, null)));
			ret.setOutputMapping(outputMappingFromString(xattribs.getString(XML_OUTPUT_MAPPING_ATTRIBUTE, null)));

			ret.setOutputMapping(outputMappingFromString(xattribs.getString(XML_OUTPUT_MAPPING_ATTRIBUTE, null)));
			ret.setAdditionalProperties(additionalPropertiesFromString(xattribs.getString(XML_ADDITIONAL_PROPERTIES_ATTRIBUTE, null)));

			return ret;
		} catch (Exception ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ") + ":" + ex.getMessage(), ex);
		}
	}

	private static List<PortMapping> importMappingFromString(String mappingString) {
		return mappingFromString(mappingString, INPUT_MAPPING_PATTERN, 1, 2);
	}

	private static List<PortMapping> outputMappingFromString(String mappingString) {
		return mappingFromString(mappingString, OUTPUT_MAPPING_PATTERN, 2, 1);
	}

	private static List<PortMapping> mappingFromString(String mappingString, Pattern pattern, int portValueIndex,
			int dictKeyValueIndex) {
		if (mappingString == null) {
			return null;
		} else {
			final List<PortMapping> ret = new LinkedList<PortMapping>();

			final String[] lines = mappingString.split("\n");
			for (String line : lines) {
				if (line.trim().length() != 0) {
					final Matcher matcher = pattern.matcher(line);
					if (!matcher.matches()) {
						throw new IllegalArgumentException("input port mapping '" + line + "' don't match patter " + pattern.pattern());
					} else {
						final String portString = matcher.group(portValueIndex);
						final String dictKeyString = matcher.group(dictKeyValueIndex);
						final PortMapping m = new PortMapping(Integer.parseInt(portString), dictKeyString);
						ret.add(m);
					}
				}
			}

			return ret;
		}
	}

	private static Properties additionalPropertiesFromString(String propertiesString) throws IOException {
		if (StringUtils.isEmpty(propertiesString)) {
			return null;
		} else {
			final Properties ret = new Properties();
//TODO			ret.load(StringUtils.getInputStream(propertiesString));
			return ret;
		}
	}

	@Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		checkPortNumbers(status, getInPorts().size(), inputMapping, "input");
		checkPortNumbers(status, getOutPorts().size(), outputMapping, "output");

		checkDictKeys(status, inputMapping, "input");
		checkDictKeys(status, outputMapping, "output");

		return status;
	}

	private void checkPortNumbers(ConfigurationStatus status, int currentInputPortCount, List<PortMapping> mapping,
			String type) {
		final PortMapping[] array = new PortMapping[currentInputPortCount];

		for (Iterator<PortMapping> it = mapping.iterator(); it.hasNext();) {
			final PortMapping m = it.next();
			if (m.getPortNumber() >= currentInputPortCount) {
				status.add(new ConfigurationProblem(type + "appings points to undefined port " + m.getPortNumber(), Severity.WARNING, this, Priority.NORMAL));
				it.remove();
			}
			if (array[m.getPortNumber()] != null) {
				status.add(new ConfigurationProblem("two mappings points to same " + type + " port " + m.getPortNumber(), Severity.ERROR, this, Priority.NORMAL));
			}
			array[m.getPortNumber()] = m;
		}

		for (int i = 0; i < array.length; i++) {
			if (array[i] == null) {
				status.add(new ConfigurationProblem(type + "port " + i + " is not mapped ", Severity.WARNING, this, Priority.NORMAL));
			}
		}
	}

	private void checkDictKeys(ConfigurationStatus status, List<PortMapping> mapping, String type) {
		final Set<String> set = new HashSet<String>();
		for (PortMapping m : mapping) {
			if (set.contains(m.getDictionaryKey())) {
				status.add(new ConfigurationProblem("two " + type + " mappings points to same dictionary key " + m.getDictionaryKey(), Severity.ERROR, this, Priority.NORMAL));
			}
		}

	}

	public String getType() {
		return COMPONENT_TYPE;
	}

	/**
	 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz) (c) Javlin Consulting (www.javlinconsulting.cz)
	 * 
	 * @created 17 Aug 2008
	 */
	private class PortWriter implements Runnable {
		private PortMapping portMapping;
		private InputPort inputPort;
		// private PipedOutputStream

		private DataRecord dataRecord;

		public PortWriter(PortMapping portMapping, InputPort inputPort) {
			this.portMapping = portMapping;
			this.inputPort = inputPort;

			dataRecord = new DataRecord(inputPort.getMetadata());
			dataRecord.init();

		}

		public void run() {
			try {
				while (inputPort.readRecord(dataRecord) != null && runIt) {

					SynchronizeUtils.cloverYield();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}

	public List<PortMapping> getInputMapping() {
		return inputMapping;
	}

	public void setInputMapping(List<PortMapping> inputMapping) {
		this.inputMapping = inputMapping;
	}

	public List<PortMapping> getOutputMapping() {
		return outputMapping;
	}

	public void setOutputMapping(List<PortMapping> outputMapping) {
		this.outputMapping = outputMapping;
	}

	public Properties getAdditionalProperties() {
		return additionalProperties;
	}

	public void setAdditionalProperties(Properties additionalProperties) {
		this.additionalProperties = additionalProperties;
	}
}
