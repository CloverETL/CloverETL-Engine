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
package org.jetel.ctl;

import java.io.InputStream;
import java.io.Reader;
import java.util.List;

import org.jetel.ctl.ASTnode.CLVFStart;
import org.jetel.ctl.ASTnode.CLVFStartExpression;

public interface ITLCompiler {

	/**
	 * Validate given (Filter) expression
	 * @param code
	 * @return list of error messages (empty when no errors)
	 */
	public List<ErrorMessage> validateExpression(String code);

	/**
	 * Validate given (Filter) expression stored in the InputStream
	 * @param input
	 * @return list of error messages (empty when no errors)
	 */
	public List<ErrorMessage> validateExpression(InputStream input);	
	
	/**
	 * Validate given (Filter) expression stored in the InputStream
	 * @param input
	 * @return list of error messages (empty when no errors)
	 */
	public List<ErrorMessage> validateExpression(Reader input);	
	
	/**
	 * Validate complex CTL code
	 * @param code
	 * @return	list of error messages (empty when no errors)
	 */
	public List<ErrorMessage> validate(String code);
	
	/**
	 * Validate complex CTL code stored in the input stream.
	 * Can be called repeatedly with different input stream.
	 * @param input
	 * @return list of error messages (empty when no errors)
	 */
	public List<ErrorMessage> validate(InputStream input);	
	
	/**
	 * Validate complex CTL code stored in the input stream.
	 * Can be called repeatedly with different input stream.
	 * @param input
	 * @return list of error messages (empty when no errors)
	 */
	public List<ErrorMessage> validate(Reader input);	
	
	/** 
	 * Sets tabulator size (in characters) for the parser to correctly calculate
	 * error position.
	 */
	public void setTabSize(int size);
	
	/**
	 * @return	Expression AST root created during {@link #validateExpression(InputStream)}
	 */
	public CLVFStartExpression getExpression();
	
	/**
	 * @return	AST root created during {@link #validate(InputStream)}
	 */
	public CLVFStart getStart();
	
	/**
	 * @return Number of critical errors from the last validate call.
	 * 
	 */
	public int errorCount();
	
	/**
	 * @return List of errors/warnings from the last validate call.
	 */
	public List<ErrorMessage> getDiagnosticMessages();

	public int warningCount();
	
	public List<ErrorMessage> compile(String code, Class<?> targetInterface, String componentId);

	/**
	 * Compiles CTL expression by wrapping it into synthetic function with specified name and return type,
	 * then compiles the resulting code by calling {@link #compile(String, Class, String)}.
	 * 
	 * @param code	CTL expression
	 * @param targetInterface	Java interface into which the code should be compiled.
	 * @param componentId	Identifier of calling component (will become part of Java class name)
	 * @param syntheticFunctionName	Name of synthetic function to create.
	 * @param syntheticReturnType	Expected type of expression (as well as return type of synthetic function).
	 * @return Compilation error messages as return by {@link #compile(String, Class, String)}.
	 */
	public List<ErrorMessage> compileExpression(String expression, Class<?> targetInterface, String componentId, String syntheticFunctionName, Class<?> syntheticReturnType);

	public Object getCompiledCode();

	/**
	 * Converts a given CTL source code to Java source code in case the compiler supports CTL-to-Java conversion.
	 *
	 * @param ctlCode the CTL source code to be converted
	 * @param targetInterface a Java interface into which the code should be compiled
	 *
	 * @return the Java source code as a string
	 *
	 * @throws UnsupportedOperationException if the compiler does not support CTL-to-Java conversion
	 * @throws ErrorMessageException if an error occurred
	 */
	public String convertToJava(String ctlCode, Class<?> targetInterface, String componentId) throws ErrorMessageException;

}
