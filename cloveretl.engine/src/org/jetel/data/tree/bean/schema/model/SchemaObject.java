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

package org.jetel.data.tree.bean.schema.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.jetel.data.tree.bean.BeanConstants;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 20.10.2011
 */
public abstract class SchemaObject implements Cloneable {

	protected SchemaObject parent;
	private String name;
	protected String type;

	protected SchemaObject(SchemaObject parent) {
		this.parent = parent;
	}

	public SchemaObject getParent() {
		return parent;
	}

	/**
	 * @param parent
	 *            the parent to set
	 */
	public void setParent(SchemaObject parent) {
		this.parent = parent;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	protected void checkParent(SchemaObject schemaObject) throws IllegalArgumentException {
		if (schemaObject.getParent() != this) {
			throw new IllegalArgumentException("Object has different parent, this: " + this + ", object's: " + schemaObject.getParent());
		}
	}

	/**
	 * Accepts visitor in depth-first fashion.
	 * 
	 * @param visitor
	 */
	public abstract void acceptVisitor(SchemaVisitor visitor);

	public abstract SchemaObject[] getChildren();

	public abstract boolean hasChildren();

	/**
	 * Compares structure of this object with other one's. Two <code>SchemaObject</code>'s are
	 * said to be equal iff they are instances of the same class, have common type (if any), name
	 * (if any) and the same holds for all of their children (order of children is significant).
	 * Note that the comparison is made only down the tree: referenced types (<code>TypedObject</code>s) need to be compared separately.
	 * @param otherObject
	 * @return
	 */
	public boolean isEqual(SchemaObject otherObject) {

		return isEqual(otherObject, true);
	}
	
	public boolean isEqual(SchemaObject otherObject, boolean childOrderSignificant) {

		if (otherObject == null) {
			return false;
		}
		if (otherObject == this) {
			return true;
		}
		if (getClass() != otherObject.getClass()) {
			return false;
		}
		if (name == null) {
			if (otherObject.name != null) {
				return false;
			}
		} else {
			if (!name.equals(otherObject.name)) {
				return false;
			}
		}
		if (type == null) {
			if (otherObject.type != null) {
				return false;
			}
		} else {
			if (!type.equals(otherObject.type)) {
				return false;
			}
		}
		if (hasChildren() != otherObject.hasChildren()) {
			return false;
		}
		SchemaObject thisChildren[] = getChildren();
		SchemaObject otherChildren[] = otherObject.getChildren();
		if (thisChildren == null || otherChildren == null) {
			return thisChildren == otherChildren;
		}
		if (thisChildren.length != otherChildren.length) {
			return false;
		}
		if (childOrderSignificant) {
			if (!equalsContentInOrder(thisChildren, otherChildren)) {
				return false;
			}
		} else {
			if (!equalsContent(thisChildren, otherChildren)) {
				return false;
			}
		}
		return true;
	}
	
	protected boolean equalsContent(SchemaObject objects1[], SchemaObject objects2[]) {
		
		List<SchemaObject> l1 = new ArrayList<SchemaObject>(Arrays.asList(objects1));
		List<SchemaObject> l2 = new ArrayList<SchemaObject>(Arrays.asList(objects2));
		for (SchemaObject o1 : l1) {
			boolean equalFound = false;
			for (Iterator<SchemaObject> i = l2.iterator(); i.hasNext();) {
				if (i.next().isEqual(o1)) {
					equalFound = true;
					i.remove();
				}
			}
			if (!equalFound) {
				return false;
			}
		}
		return true;
	}
	
	protected boolean equalsContentInOrder(SchemaObject objects1[], SchemaObject objects2[]) {
		
		for (int i = 0; i < objects1.length; ++i) {
			if (objects1[i] == null) {
				if (objects2[i] != null) {
					return false;
				}
			} else {
				if (!objects1[i].isEqual(objects2[i])) {
					return false;
				}
			}
		}
		return true;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public String getPath() {
		String path = "";
		if (parent != null) {
			/*
			 * my path is parent path + relation to the parent
			 */
			path += parent.getPath();
			path += parent.getRelationPath(this);
		} else {
			/*
			 * top level
			 */
			path += (BeanConstants.PATH_SEPARATOR + getPathName());
		}
		return path;
	}
	
	protected String getPathName() {
		return name != null ? name : getDefaultName();
	}

	public abstract String getDefaultName();
	
	public String getRelationPath(SchemaObject schemaObject) {
		return "";
	}
	
	@Override
	public String toString() {
		String path = getPath();
		if (path != null && path.length() > 0) {
			return path;
		}
		return super.toString();
	}
	
	/**
	 * Answers shallow copy of the <code>SchemaObject</code>.
	 * 
	 * @see SchemaObjectReplicator
	 */
	@Override
	public SchemaObject clone() {
		try {
			return (SchemaObject)super.clone();
		} catch (CloneNotSupportedException e) {
			throw new InternalError();
		}
	}
}
