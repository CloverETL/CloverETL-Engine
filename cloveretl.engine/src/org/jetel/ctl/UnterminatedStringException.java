package org.jetel.ctl;

/**
 * Exception thrown by token manager in case an unterminated string literal is found 
 * during parsing.
 * 
 * @author mtomcanyi
 *
 */
public class UnterminatedStringException extends RuntimeException {

	private static final long serialVersionUID = 5198861334756655466L;

	
	public final int beginLine;
	public final int beginColumn;
	public final int endLine;
	public final int endColumn;
	
	public UnterminatedStringException(int beginLine, int beginColumn, int endLine, int endColumn) {
		this.beginLine = beginLine;
		this.beginColumn = beginColumn;
		this.endLine = endLine;
		this.endColumn = endColumn;
	}
	
}
