/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/

// FILE: c:/projects/jetel/org/jetel/exception/NoMoreDataException.java

package org.jetel.exception;

import org.jetel.graph.IGraphElement;
import org.jetel.util.StringUtils;

/** A class that represents general Jetel exception
 * 
 * @see Exception
 * @author D.Pavlis
 */
public class ComponentNotReadyException extends Exception {
  
  // Attributes
  IGraphElement graphElement;
  
  String attributeName;
  // Associations

  // Operations
  public ComponentNotReadyException(String message){
	  super(message);
  }
  
  public ComponentNotReadyException(Exception ex){
      super(ex);
  }

  public ComponentNotReadyException(IGraphElement element,Exception ex){
      super(ex);
      this.graphElement=element;
  }
  
  public ComponentNotReadyException(IGraphElement element,String message,Exception ex){
      super(message,ex);
      this.graphElement=element;
  }
  
  public ComponentNotReadyException(IGraphElement element,String message){
      super(message);
      this.graphElement=element;
  }

  public ComponentNotReadyException(IGraphElement element, String attributeName, String message) {
      super(message);
      this.graphElement = element;
      this.attributeName = attributeName;
  }

  public IGraphElement getGraphElement(){
      return graphElement;
  }

  public String getAttributeName() {
      return attributeName;
  }

  public void setAttributeName(String attributeName) {
      this.attributeName = attributeName;
  }
  
    public String toString() {
        StringBuffer message = new StringBuffer(80);
        if (graphElement!=null){
            message.append("Element [").append(graphElement.getId()).append(':');
            if (graphElement.getName()!=null) message.append(graphElement.getName());
            message.append("]-");
        }
        if(!StringUtils.isEmpty(attributeName)) {
            message.append("[attribute = " + attributeName + "]-");
        }
        message.append(super.getMessage());
        return message.toString();
    }

} /* end class NoMoreDataException */
