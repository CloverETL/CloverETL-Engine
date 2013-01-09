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
package org.jetel.exception;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.jetel.graph.runtime.TempSpace;

/**
 * Custom exception for problems occurring when temp. file is being created.
 * 
 * @author "Michal Oprendek" (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 21.3.2012
 */
public class TempFileCreationException extends Exception {

	private static final long serialVersionUID = 8694810847861904826L;

	/**
	 * Reason for the exception:
	 * 
	 * <ul>
	 * <li>NO_SPACE_AVAILABLE – temp file can not be created because there is no space on any of devices; not recognized
	 * in standalone engine</li>
	 * <li>IO_EXCEPTION – generic I/O exception without further detail</li>
	 * </ul>
	 * 
	 * @author "Michal Oprendek" (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 * 
	 * @created 21.3.2012
	 */
	public enum Reason {
		NO_SPACE_AVAILABLE, NO_TEMPSPACE_CONFIGURED, MIXED, OTHER
	}

	private final Map<TempSpace, Throwable> causes = new LinkedHashMap<TempSpace, Throwable>();
	private Reason reason;

	/**
	 * Creates new TempFileCreationException. Intended for cases when there is only one cause and it is not and
	 * {@link Throwable}.
	 * 
	 * @param message
	 *            message detailing failure cause
	 * @param reason
	 *            reason for the failure
	 * @param label
	 *            label from temp file creation request
	 * @param allocationHint
	 *            allocation hint from temp file creation request
	 * @param runId
	 *            run id of the graph; may be <code>null</code> if not available
	 */
	public TempFileCreationException(String message, Reason reason, String label, int allocationHint, Long runId) {
		super(String.format("Creation of temp. space with label '%s', hint=%d failed for graph run id=%d; with reason: %s %s",
				label, allocationHint, runId, reason, message == null ? "" : "and message: " + message));
		this.reason = reason;
	}

	/**
	 * Creates new TempFileCreationException. Next causes can be added later by {@link #addCause(Throwable, TempSpace)}
	 * method calls
	 * 
	 * @param cause
	 *            lower-level-of-abstraction {@link Throwable} that caused this exception
	 * @param label
	 *            label from temp file creation request
	 * @param allocationHint
	 *            allocation hint from temp file creation request
	 * @param runId
	 *            run id of the graph; may be <code>null</code> if not available
	 * @param tempSpace
	 *            temp space where temp file creation attempt has been performed
	 */
	public TempFileCreationException(Throwable cause, String label, int allocationHint, Long runId, TempSpace tempSpace) {
		super(String.format("Creation of temp. space with label '%s', hint=%d failed for graph run id=%d", label, allocationHint, runId), cause);
		if (cause != null) {
			causes.put(tempSpace, cause);
		}
	}

	/**
	 * <p>
	 * Add next cause to the exception (this kind of exceptions can have multiple causes that are necessary but not
	 * sufficient conditions of the failure – only the combination is sufficient condition).
	 * </p>
	 * 
	 * <p>
	 * This method is not valid if reason is already set (by
	 * {@link #TempFileCreationException(String, Reason, String, int, Long)} constructor)
	 * </p>
	 * 
	 * @param t
	 *            throwable that co-caused the exception
	 * @param tempSpace
	 *            temp space where temp file creation attempt leading to t has been performed
	 */
	public void addCause(Throwable t, TempSpace tempSpace) {
		if (t == null) {
			throw new IllegalArgumentException("param. Throwable t can not be null");
		}
		if (tempSpace == null) {
			throw new IllegalArgumentException("param. tempSpace can not be null");
		}
		if (reason != null) {
			throw new IllegalStateException("causes can not be added to exception that has already reason set");
		}
		if (causes.containsKey(tempSpace)) {
			throw new IllegalArgumentException("exception already logged for tempsSpace " + tempSpace);
		}
		causes.put(tempSpace, t);
	}

	/**
	 * @return the reason
	 */
	public Reason getReason() {
		return reason;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(super.toString());
		if (!causes.isEmpty()) {
			boolean multipleCauses = causes.size() > 1;
			sb.append("\n\tThe temp file creation error has " + causes.size() + " cause" + (multipleCauses ? "s" : "") + ":\n");
			int i = 1;
			for (Entry<TempSpace, Throwable> cause : causes.entrySet()) {
				sb.append("\t\t");
				if (multipleCauses) {
					sb.append("Cause " + i++ + (cause.getKey() == null ? "" : "; in " + cause.getKey()) + ": ");
				}
				sb.append(cause.getValue() + "\n");
			}
		}
		return sb.toString();
	}

}
