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
package org.jetel.graph.runtime;

import org.jetel.graph.IGraphElement;
import org.jetel.graph.Node;

/**
 * @author david pavlis
 * @since  13.12.2006
 *
 */
public class Message<T> implements Comparable<Message>{
    
    public enum Type {
        MESSAGE,
        NODE_FINISHED,
        ERROR
    }
    
    protected Type type;
    protected IGraphElement sender;
    protected IGraphElement recipient;
    protected T body;
    protected int priority;
    
    /**
     * @param senderID
     * @param senderThreadID
     * @param recipientID
     * @param type
     * @param body
     */
    public Message(IGraphElement sender, IGraphElement recipient, Type type, T body) {
        this.type = type;
        this.body = body;
        this.sender = sender;
        this.recipient = recipient;
        this.priority = type.ordinal();
    }

    
    public Message(IGraphElement sender, IGraphElement recipient, Type type) {
        this(sender, recipient, type, null);
    }
    
    
    public static Message<ErrorMsgBody> createErrorMessage(Node node, ErrorMsgBody exception) {
        return new Message<ErrorMsgBody>(node, null, Type.ERROR, exception);
    }
    
    public static Message<Void> createNodeFinishedMessage(Node node) {
    	return new Message<Void>(node, null, Type.NODE_FINISHED);
    }

    public static Message<Object> createMessage(Node node, Object messageBody) {
        return new Message<Object>(node, null, Type.MESSAGE, messageBody);
    }

    /**
     * @return the body
     * @since 13.12.2006
     */
    public T getBody() {
        return body;
    }

    /**
     * @param body the body to set
     * @since 13.12.2006
     */
    public void setBody(T body) {
        this.body = body;
    }

    /**
     * @return the recipientID
     * @since 13.12.2006
     */
    public IGraphElement getRecipient() {
        return recipient;
    }

    /**
     * @param recipientID the recipientID to set
     * @since 13.12.2006
     */
    public void setRecipient(IGraphElement recipient) {
        this.recipient = recipient;
    }

    /**
     * @return the senderID
     * @since 13.12.2006
     */
    public IGraphElement getSender() {
        return sender;
    }

    /**
     * @param senderID the senderID to set
     * @since 13.12.2006
     */
    public void setSender(IGraphElement sender) {
        this.sender = sender;
    }

    /**
     * @return the type
     * @since 13.12.2006
     */
    public Type getType() {
        return type;
    }

    /**
     * @param type the type to set
     * @since 13.12.2006
     */
    public void setType(Type type) {
        this.type = type;
    }
    
    @Override
	public int compareTo(Message to){
        int result=this.type.compareTo(to.type);
        if (result==0){
            result= this.priority-to.priority;
        }
        return result;
    }
    
    @Override public String toString(){
        StringBuilder str=new StringBuilder(40);
        str.append("Sender:").append(sender != null ? sender.getId() : "null").append("\n");
        str.append("Recipient:").append(recipient != null ? recipient.getId() : "null").append("\n");
        str.append("Type:").append(type).append("\n");
        return str.toString();
    }


    /**
     * @return the priority
     * @since 10.1.2007
     */
    public int getPriority() {
        return priority;
    }


    /**
     * @param priority the priority to set
     * @since 10.1.2007
     */
    public void setPriority(int priority) {
        this.priority = priority;
    }
    
}
