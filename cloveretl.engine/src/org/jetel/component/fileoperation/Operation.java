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
package org.jetel.component.fileoperation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Operation {
	
	public final OperationKind kind;
	
	protected final List<String> schemes;
	
	public Operation(OperationKind kind, String... schemes) {
		this.kind = kind;
		this.schemes = Collections.unmodifiableList(Arrays.asList(schemes));
	}
	
	public String scheme() {
		return schemes.get(0);
	}
	
	public String scheme(int i) {
		return schemes.get(i);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((kind == null) ? 0 : kind.hashCode());
		result = prime * result + ((schemes == null) ? 0 : schemes.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Operation other = (Operation) obj;
		if (kind != other.kind)
			return false;
		if (schemes == null) {
			if (other.schemes != null)
				return false;
		} else if (!schemes.equals(other.schemes))
			return false;
		return true;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("%s %s", kind, schemes); //$NON-NLS-1$
	}

	public static class OneParameterOperation extends Operation {

		public OneParameterOperation(OperationKind kind, String scheme) {
			super(kind, scheme);
		}
		
		public String getScheme() {
			return schemes.get(0);
		}

	}

	public static class TwoParameterOperation extends Operation {

		public TwoParameterOperation(OperationKind kind, String source, String target) {
			super(kind, source, target);
		}
		
		public String getSource() {
			return schemes.get(0);
		}

		public String getTarget() {
			return schemes.get(1);
		}

	}


	public static OneParameterOperation read(String scheme) {
		return new OneParameterOperation(OperationKind.READ, scheme);
	}

	public static OneParameterOperation write(String scheme) {
		return new OneParameterOperation(OperationKind.WRITE, scheme);
	}

	public static OneParameterOperation delete(String scheme) {
		return new OneParameterOperation(OperationKind.DELETE, scheme);
	}

	public static OneParameterOperation list(String scheme) {
		return new OneParameterOperation(OperationKind.LIST, scheme);
	}

	public static OneParameterOperation resolve(String scheme) {
		return new OneParameterOperation(OperationKind.RESOLVE, scheme);
	}

	public static OneParameterOperation create(String target) {
		return new OneParameterOperation(OperationKind.CREATE, target);
	}

	public static OneParameterOperation info(String target) {
		return new OneParameterOperation(OperationKind.INFO, target);
	}

	public static OneParameterOperation file(String target) {
		return new OneParameterOperation(OperationKind.FILE, target);
	}

	public static TwoParameterOperation copy(String source, String target) {
		return new TwoParameterOperation(OperationKind.COPY, source, target);
	}

	public static TwoParameterOperation move(String source, String target) {
		return new TwoParameterOperation(OperationKind.MOVE, source, target);
	}
}
