package org.jetel.mapping;


/**
 * Contains source and target statement of mapping statement.
 * 
 * @author Martin Zatopek (martin.zatopek@opensys.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *         
 * @comments Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *
 * @created 14.8.2007
 */
public class MappingAssignment {
	
	// assignment delimiter
	private static final String ELEMENT_DELIMITER_REGEX = ":=";

	// source - right side
	private MappingSource source;
	
	// target - left side
	private MappingTarget target;
	
	/**
	 * Returns source of mapping assignment.
	 * 
	 * @return - source
	 */
	public MappingSource getSource() {
		return source;
	}

	/**
	 * Returns target of mapping assignment.
	 * 
	 * @return - target
	 */
	public MappingTarget getTarget() {
		return target;
	}

	/**
	 * Sets source of mapping assignment.
	 * 
	 * @param source
	 */
	public void setSource(MappingSource source) {
		this.source = source;
	}

	/**
	 * Sets target of mapping assignment.
	 * 
	 * @param target
	 */
	public void setTarget(MappingTarget target) {
		this.target = target;
	}

	/**
	 * Parses and gets a mapping assignment.
	 * 
	 * @param rawStatement - string containing a mapping assignment
	 * @return - MappingAssignment
	 * @throws MappingException
	 */
	public static MappingAssignment createAssignment(String rawStatement, IMappingElementProvider mappingProvider) throws MappingException {
		MappingAssignment assignment = new MappingAssignment();
		
		// split assignment
		String[] elements = rawStatement.split(ELEMENT_DELIMITER_REGEX, 2); //better way - check ':=' for ctl data elements
		
		// assignment must have just two side
		if(elements.length < 2) {
			throw new MappingException("Invalid mapping statement: '" + rawStatement + "'.");
		}
	
		// set source and target
		assignment.setSource(mappingProvider.createMappingSource(elements[1].trim()));
		assignment.setTarget(mappingProvider.createMappingTarget(elements[0].trim()));
		
		return assignment;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(target.toString()).append(ELEMENT_DELIMITER_REGEX).append(source.toString());
		return sb.toString();
	}
}
