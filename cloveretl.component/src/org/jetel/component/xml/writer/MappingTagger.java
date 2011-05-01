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
package org.jetel.component.xml.writer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.component.xml.writer.mapping.MappingProperty;
import org.jetel.component.xml.writer.mapping.Element;
import org.jetel.component.xml.writer.mapping.Relation;
import org.jetel.component.xml.writer.mapping.XmlMapping;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Visitor which resolves which elements takes records from which input port, input port mode and partition element.
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 26 Jan 2011
 */
public class MappingTagger extends AbstractVisitor {
	
	private static final String REASON_MULTIPLE_USAGE = "Multiple usage of input port.";
	private static final String REASON_UNSORTED_PARENT = "Data from parent input port are not sorted. Minimal required sort order: ";
	private static final String REASON_UNSORTED_PORT = "Data from input port are not sorted. Minimal required sort order: ";
	private static final String REASON_NOT_IN_PARTITION = "Input port is used outside of the partition scope.";
	private static final String REASON_PARENT_CACHED = "Data from parent data port are cached.";
	private static final String REASON_NO_RELATION = "No relation (key - parent key pair) is specified.";
	private static final String REASON_WRONG_SORT_PORT = "Data from input port are in unsuitable sort order. Minimal required sort order: ";
	private static final String REASON_WRONG_SORT_PARENT = "Data from parent input port are in unsuitable sort order. Minimal required sort order: ";
	private static final String REASON_UNMATCHED_SORT = "Data from parent input port are in unsuitable sort order. Detail: ";
	

	private final Map<Integer, DataRecordMetadata> inPorts;
	private final Map<Integer, SortHint> sortHints;
	
	private Stack<Integer> availableData = new Stack<Integer>();

	private Element partitionElement = null;
	private Element partitionElementCandidate = null;
	private Map<Element, Tag> tagMap = new HashMap<Element, Tag>();

	private Map<Integer, PortTag> portTagMap = new HashMap<Integer, PortTag>();
	private boolean resolvePartition = false;
	
	public MappingTagger(Map<Integer, DataRecordMetadata> inPorts, String sortHintsString) {
		this.inPorts = inPorts;
		this.sortHints = resolveSortHints(sortHintsString, inPorts.keySet());
	}

	public void tag() {
		clear();
		mapping.visit(this);
	}
	
	public void clear() {
		partitionElement = null;
		partitionElementCandidate = null;
		tagMap.clear();
		portTagMap.clear();
	}
	
	public Element getPartitionElement() {
		return partitionElement != null ? partitionElement : partitionElementCandidate;
	}
	
	public Set<Integer> getUsedPorts() {
		return portTagMap.keySet();
	}

	public Map<Integer, PortData> getPortDataMap(Map<Integer, InputPort> inPorts, String tempDirectory, long cacheSize) throws ComponentNotReadyException {
		Map<Integer, PortData> portDataMap = new HashMap<Integer, PortData>(inPorts.size());
		for (Entry<Integer, InputPort> entry : inPorts.entrySet()) {
			Integer inPortIndex = entry.getKey();
			
			PortTag portTag = portTagMap.get(inPortIndex);
			if (portTag != null) {
				PortData portData = PortData.getInstance(portTag.isCached(), entry.getValue(),
						portTag.getKeys(), sortHints.get(inPortIndex), tempDirectory, cacheSize);
				portDataMap.put(inPortIndex, portData);
			}
		}
		return portDataMap;
	}
	
	private boolean inPartition(Element element) {
		if (partitionElement != null || partitionElementCandidate != null) {
			Element parent = element;
			while (parent != null) {
				if (parent.isPartition()) {
					return true;
				}
				parent = getRecurringParent(parent);
			}
		}
		return false;
	}
	
	public int getPartitionElementPortIndex() {
		Element partitionElement = getPartitionElement();
		if (partitionElement != null) {
			return tagMap.get(partitionElement).getPortIndex();
		}
		return -1;
	}
	
	public Map<Element, Tag> getTagMap() {
		return tagMap;
	}
	
	public Tag getTag(Element key) {
		return tagMap.get(key);
	}
	
	public boolean isCached(Integer portIndex) {
		return portTagMap.get(portIndex).isCached();
	}
	
	public boolean isPortAvailable(Element element, int portIndex) {
		Tag tag = getTag(element);
		if (tag != null && tag.getPortIndex() == portIndex) {
			return true;
		}
		if (element.getParent() != null) {
			return isPortAvailable(element.getParent(), portIndex);
		}
		return false;
	}
	
	public void setMapping(XmlMapping mapping) {
		super.setMapping(mapping);
		clear();
	}

	@Override
	public void visit(Element element) throws Exception {
		boolean recurring = false;
		if (!isInRecursion()) {
			recurring = resolveIndex(element);
			collectPartitionInformation(element);
		}
		updatePortModeInformation(element);
		visitChildren(element);
		if (recurring) {
			availableData.pop();
		}
	}

	private void updatePortModeInformation(Element element) {
		Tag tag = tagMap.get(element);
		if (tag == null || tag.getPortIndex() == null) {
			return;
		}
		
		Integer usedPortIndex = tag.getPortIndex();
		if (isInRecursion()) {
			setPortTag(usedPortIndex, true, REASON_MULTIPLE_USAGE);
			return;
		}

		List<String> key = null;
		
		Relation recurringInfo = element.getRelation();
		if (recurringInfo != null) {
			String keyString = recurringInfo.getProperty(MappingProperty.KEY);
			if (keyString != null) {
				key = Arrays.asList(keyString.split(XmlMapping.DELIMITER));
			}
		}

		if (!portTagMap.containsKey(usedPortIndex) || !isCached(usedPortIndex)) {
			Element parent = element;
			do { // looking for parent
				parent = parent.getParent();
				if (parent == null) { // root loop element
					if (portTagMap.containsKey(usedPortIndex)) {
						setPortTag(usedPortIndex, true, REASON_MULTIPLE_USAGE);
					} else if (resolvePartition && !inPartition(element)) {
						setPortTag(usedPortIndex, true, REASON_NOT_IN_PARTITION);
					} else {
						setPortTag(usedPortIndex, false, null);
					}
					break;
				}

				Tag parentTag = tagMap.get(parent);
				if (parentTag != null) { // Is it loop element?
					Integer parentInputPortIndex = parentTag.getPortIndex();
					boolean parentCached = portTagMap.get(parentInputPortIndex).isCached();
					SortHint sortHint = sortHints.get(usedPortIndex);
					SortHint parentSortHint = sortHints.get(parentInputPortIndex);
					
					List<String> parentKey = null;
					if (recurringInfo != null) {
						String parentKeysString = recurringInfo.getProperty(MappingProperty.PARENT_KEY);
						if (parentKeysString != null) {
							parentKey = Arrays.asList(parentKeysString.split(XmlMapping.DELIMITER));
						}
					}					
					
					String reason = null;
					if (portTagMap.containsKey(usedPortIndex)) {
						reason = REASON_MULTIPLE_USAGE;
					} else if (parentKey == null) {
						reason = REASON_NO_RELATION;
					} else if (parentSortHint == null) {
						reason = REASON_UNSORTED_PARENT + parentKey;
					} else if (parentCached) {
						reason = REASON_PARENT_CACHED; 
					} else if (sortHint == null) {
						reason = REASON_UNSORTED_PORT + key;
					}
					if (reason != null) {
						setPortTag(usedPortIndex, true, reason);
						break;
					}
					
					if (!isPrefix(sortHint.getKeyFields(), key)) {
						reason = REASON_WRONG_SORT_PORT + key;
					} else if (parentSortHint.getKeyFields().length < parentKey.size()) {
						reason = REASON_WRONG_SORT_PARENT + parentKey;
					} else {
						String[] parentKeyFields = parentSortHint.getKeyFields();
						boolean[] parentSortOrder = parentSortHint.getAscending();
						boolean[] sortOrder = sortHint.getAscending();
						for (int i = 0; i < parentKeyFields.length; i++) {
							if (!parentKeyFields[i].equals(parentKey.get(i))) {
								reason = REASON_UNMATCHED_SORT + i + ". field is '" 
									+ parentKeyFields[i] + "' but '" + parentKey.get(i) 
									+ "' is required.";
								break;
							} else if (parentSortOrder[i] != sortOrder[i]) {
								reason = REASON_UNMATCHED_SORT + i + ". field is '" 
									+ (parentSortOrder[i] ? "ascending" : "descending") 
									+ "' but '" + (parentSortOrder[i] ? "descending" : "ascending") 
									+ "' is required.";
								break;
							}
						}
					}
					
					if (reason != null) {
						setPortTag(usedPortIndex, true, reason);
					} else if (resolvePartition && !inPartition(element)) {
						setPortTag(usedPortIndex, true, REASON_NOT_IN_PARTITION);
					} else {
						setPortTag(usedPortIndex, false, null);
					}
					break;
				}
			} while (tagMap.get(parent) == null); //looking for parent loop element
		}

		getPortTagInternal(usedPortIndex).addKey(key);
	}

	private boolean isPrefix(String[] list, List<String> prefix) {
		if (list.length < prefix.size()) {
			return false;
		}
		for (int i = 0; i < prefix.size(); i++) {
			if (!prefix.get(i).equals(list[i])) {
				return false;
			}
		}
		return true;
	}

	private void collectPartitionInformation(Element element) {
		if (resolvePartition && partitionElement == null) {
			if (element.isPartition()) {
				partitionElement = element;
			} else if (partitionElementCandidate == null) {
				if (element.getRelation() != null || tagMap.get(element) != null) {
					partitionElementCandidate = element;
				}
			}
		}

	}

	private boolean resolveIndex(Element element) {
		if (element.getRelation() != null) {
			String inPortString = element.getRelation().getProperty(MappingProperty.INPUT_PORT); 
			if (inPortString != null) {
				Integer inputPortIndex = getFirstPortIndex(inPortString, inPorts);
				availableData.push(inputPortIndex);
				tagMap.put(element, new Tag(inputPortIndex));
				return true;
			}
		}
		return false;
	}

	public void setResolvePartition(boolean resolvePartition) {
		this.resolvePartition  = resolvePartition;
	}
	
	private Map<Integer, SortHint> resolveSortHints(String sortHintsString, Collection<Integer> inPorts) {
		Map<Integer, SortHint> toReturn = new HashMap<Integer, SortHint>();
		if (sortHintsString == null) {
			return toReturn;
		}
		String[] recordHints = sortHintsString.split("#");
		Iterator<Integer> portIterator = inPorts.iterator();
		Pattern pat = Pattern.compile("^(.*)\\((.*)\\)$");
		for (int j = 0; j < Math.min(recordHints.length, inPorts.size()); j++) {
			Integer inPortNumber = portIterator.next();
			if (!recordHints[j].isEmpty()) {
				String[] sortKeys = recordHints[j].split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
				  
				boolean[] ascending = new boolean[sortKeys.length];
		        
		        for (int i = 0; i < sortKeys.length; i++) {
		        	Matcher matcher = pat.matcher(sortKeys[i]);
		        	if (matcher.find()) {
			        	String keyPart = sortKeys[i].substring(matcher.start(1), matcher.end(1));
			        	if (matcher.groupCount() > 1) {
			        		ascending[i] = (sortKeys[i].substring(matcher.start(2), matcher.end(2))).matches("^[Aa].*");	        		
			        	}
			        	sortKeys[i] = keyPart;
		        	}
		        }
		        SortHint hint = new SortHint(sortKeys, ascending);
		        toReturn.put(inPortNumber, hint);
			}
		}
		return toReturn;
	}
	
	public static class Tag {
		private Integer portIndex = null;

		private Tag(Integer portIndex) {
			this.portIndex = portIndex;
		}

		public Integer getPortIndex() {
			return portIndex;
		}

		public void setPortIndex(Integer portIndex) {
			this.portIndex = portIndex;
		}
	}
	
	private void setPortTag(Integer portIndex, boolean cached, String cachedReason) {
		PortTag portTag = getPortTagInternal(portIndex);
		portTag.setCached(cached);
		portTag.setCachedReason(cachedReason);
	}
	
	private PortTag getPortTagInternal(Integer portIndex) {
		PortTag portTag = portTagMap.get(portIndex);
		if (portTag == null) {
			portTag = new PortTag(portIndex);
			portTagMap.put(portIndex, portTag);
		}
		return portTag;
	}
	
	public PortTag getPortTag(Integer portIndex) {
		return portTagMap.get(portIndex);
	}
	
	public static class PortTag {
		private final int portIndex;
		private boolean cached;
		private String cachedReason;
		private Set<List<String>> keys = new HashSet<List<String>>(1);
		
		public PortTag(int portIndex) {
			this.portIndex = portIndex;
		}
		
		public int getPortIndex() {
			return portIndex;
		}

		public boolean isCached() {
			return cached;
		}
		
		public void setCached(boolean cached) {
			this.cached = cached;
		}

		public String getCachedReason() {
			return cachedReason;
		}

		public void setCachedReason(String cachedReason) {
			this.cachedReason = cachedReason;
		}

		public Set<List<String>> getKeys() {
			return new HashSet<List<String>>(keys);
		}
		
		public void addKey(List<String> key) {
			keys.add(key);
		}
	}

}
