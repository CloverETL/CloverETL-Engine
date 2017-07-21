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

import org.jetel.graph.GraphElement;
import org.jetel.graph.IGraphElement;
import org.jetel.util.string.StringUtils;

/** A class that represents general Jetel exception
 * 
 * @see Exception
 * @author D.Pavlis
 */
public class ComponentNotReadyException extends Exception {
  
    private static final long serialVersionUID = 1L;

// Attributes
  IGraphElement graphElement;
  
  String attributeName;
  // Associations

  // Operations
  public ComponentNotReadyException(String message){
	  super(message);
  }
  
  public ComponentNotReadyException(String message, String attributeName){
	  super(message);
	  this.attributeName = attributeName;
  }
  
  public ComponentNotReadyException(Throwable ex){
      super(ex);
  }

  public ComponentNotReadyException(String message, Throwable ex){
      super(message, ex);
  }

  public ComponentNotReadyException(IGraphElement element,Throwable ex){
      super(ex);
      this.graphElement=element;
  }
  
  public ComponentNotReadyException(IGraphElement element,String message,Throwable ex){
      super(message,ex);
      this.graphElement=element;
  }
  
  public ComponentNotReadyException(IGraphElement element,String message,Throwable ex, String attributeName){
      super(message,ex);
      this.graphElement = element;
      this.attributeName = attributeName;
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
  
  public void setGraphElement(GraphElement graphElement) {
      this.graphElement = graphElement;
  }

  public String getAttributeName() {
      return attributeName;
  }

  public void setAttributeName(String attributeName) {
      this.attributeName = attributeName;
  }
  
    @Override
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
