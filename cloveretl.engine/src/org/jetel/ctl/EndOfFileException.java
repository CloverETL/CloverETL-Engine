package org.jetel.ctl;


/**
 * This exception is thrown by {@link TransformLangParser#unknownToken} method in case
 * it parses EOF token. This is the only way how to terminate properly on EOF token and
 * distinguish from other possible {@link ParseException} thrown.
 * 
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 *
 */
public class EndOfFileException extends ParseException {

	private static final long serialVersionUID = 3000771874734137135L;

}
