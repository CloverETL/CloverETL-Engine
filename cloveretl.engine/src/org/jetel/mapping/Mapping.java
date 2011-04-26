package org.jetel.mapping;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jetel.mapping.filter.MappingFilter;
import org.jetel.util.XmlCtlDataUtil;
import org.jetel.util.string.StringUtils;


/**
 * This class is a container for mapping assignment.
 * 
 * @author Martin Zatopek (martin.zatopek@opensys.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *         
 * @comments Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *
 * @created 14.8.2007
 */
public class Mapping implements Iterable<MappingAssignment> {

	// every assignement statement is delimited by ';'
	private static final String STATEMENT_DELIMITER_REGEX = ";"; // (!\\[CTLDATA\\[.*;.*\\]\\])
	
	// inner elements of container
	private List<MappingAssignment> assignments;
	
	/**
	 * Constructor.
	 */
	private Mapping() {
		assignments = new ArrayList<MappingAssignment>();
	}
	
	/**
	 * Adds a mapping assignment into this conainer.
	 * 
	 * @param assignment - a mapping assignment
	 */
	private void addAssignment(MappingAssignment assignment) {
		assignments.add(assignment);
	}

	/**
	 * Returns count of mapping assignments in this conainer.
	 * 
	 * @return count of mapping assignments
	 */
	public int size() {
		return assignments.size();
	}

	/**
	 * Gets subset of this container in terms of the filter. The filter defines 
	 * source and target conditions that filteres mapping elements.
	 * 
	 * @param filter - filteres mapping elements
	 * @return subset of mapping
	 */
	public Mapping subMapping(MappingFilter filter) {
		Mapping mapping = new Mapping();
		
		for(MappingAssignment assignment : assignments) {
			if(filter.checkAssignment(assignment)
					&& filter.checkSource(assignment.getSource())
					&& filter.checkTarget(assignment.getTarget())) {
				mapping.addAssignment(assignment);
			}
		}
		return mapping;
	}
	
	/**
	 * Creates new mapping container from string.
	 * 
	 * @param rawMapping - a string containing mapping assignments
	 * @return mapping container
	 * @throws MappingException
	 */
	public static Mapping createMapping(String rawMapping, IMappingElementProvider mappingProvider) throws MappingException {
		Mapping mapping = new Mapping();

		// if the string contains only blank characters then return empty container
        if(StringUtils.isBlank(rawMapping)) {
            return mapping;
        }

		// create and add statement into container
        List<XmlCtlDataUtil.XmlData> lData = XmlCtlDataUtil.parseCTLData(rawMapping);
		StringBuilder sb = new StringBuilder();
		String[] statements;
        for (XmlCtlDataUtil.XmlData data: lData) {
			// assign/don't assign ctl data statement
        	if (data.isCTLData()) {
        		sb.append(XmlCtlDataUtil.createCTLDataElement(data.getData()));
        	} else {
                // split string to mapping statements
        		statements = data.getData().split(STATEMENT_DELIMITER_REGEX);
        		if (statements.length == 0) {
            		if (sb.length() > 0) {
            			mapping.addAssignment(MappingAssignment.createAssignment(sb.toString(), mappingProvider));
                		sb = new StringBuilder();
            		}
        		} 
        		else if (statements.length == 1) {
            		if (sb.length() > 0) {
            			sb.append(statements[0]).toString();
            		} else {
            			mapping.addAssignment(MappingAssignment.createAssignment(statements[0], mappingProvider));
            		}
        		}
        		else {
        			for (int i=0; i<statements.length-1;i++) {
           				mapping.addAssignment(MappingAssignment.createAssignment(
           						i==0 ? sb.append(statements[0]).toString():statements[i],
           						mappingProvider));
        			}
    				sb = new StringBuilder();
    				sb.append(statements[statements.length-1]);
        		}
			}
        }
        if (sb.length() > 0) 
        	mapping.addAssignment(MappingAssignment.createAssignment(sb.toString(), mappingProvider));
        
		return mapping;
	}

	/**
	 * Returns an iterator over this container.
	 */
	public Iterator<MappingAssignment> iterator() {
		return assignments.iterator();
	}

	public MappingAssignment getAssignment(int index) {
		return assignments.get(index);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(MappingAssignment assignment : assignments) {
			sb.append(assignment.toString()).append(STATEMENT_DELIMITER_REGEX);
		}
		return sb.toString();
	}
	
}
