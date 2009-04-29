package org.jetel.ctl;

/**
 * Specific position in CTL code represented by a pair [line, column].
 * Information about line and column comes from parser.
 * 
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 *
 */
public class SyntacticPosition {

	
	private final int line;
	private final int column;
	
	public SyntacticPosition(int line, int column) {
		super();
		this.line = line;
		this.column = column;
	}

	public int getColumn() {
		return column;
	}
	
	public int getLine() {
		return line;
	}
	
	
	
}
