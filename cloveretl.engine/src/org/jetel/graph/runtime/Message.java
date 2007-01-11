/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-06  David Pavlis <david.pavlis@centrum.cz> and others.
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
 * Created on 13.12.2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.graph.runtime;

import org.jetel.graph.Node;

/**
 * @author david pavlis
 * @since  13.12.2006
 *
 */
public class  Message<T> implements Comparable<Message>{
    
    public enum Type {
        MESSAGE,
        ERROR
    }
    
    protected Type type;
    protected String senderID;
    protected long  senderThreadID;
    protected String recipientID;
    protected T body;
    protected int priority;
    
    /**
     * @param senderID
     * @param senderThreadID
     * @param recipientID
     * @param type
     * @param body
     */
    public Message(String senderID,long senderThreadID,String recipientID,Type type,T body){
        this.type=type;
        this.body=body;
        this.senderID=senderID;
        this.senderThreadID=senderThreadID;
        this.recipientID=recipientID;
        this.priority=0;
    }

    
    public Message(Node node,String recipientID,Type type,T body){
        this(node.getId(),node.getNodeThread().getId(),recipientID,type,body);
    }
   
    public Message(Node node,String recipientID,Type type){
        this(node.getId(),node.getNodeThread().getId(),recipientID,type,null);
    }
    
    
    public static Message createErrorMessage(Node node,ErrorMsgBody exception){
        return new Message<ErrorMsgBody>(node,null,Type.ERROR,exception);
    }
    
    
    /**
     * @return the body
     * @since 13.12.2006
     */
    public T getBody() {
        return (T)body;
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
    public String getRecipientID() {
        return recipientID;
    }

    /**
     * @param recipientID the recipientID to set
     * @since 13.12.2006
     */
    public void setRecipientID(String recipientID) {
        this.recipientID = recipientID;
    }

    /**
     * @return the senderID
     * @since 13.12.2006
     */
    public String getSenderID() {
        return senderID;
    }

    /**
     * @param senderID the senderID to set
     * @since 13.12.2006
     */
    public void setSenderID(String senderID) {
        this.senderID = senderID;
    }

    /**
     * @return the senderThreadID
     * @since 13.12.2006
     */
    public long getSenderThreadID() {
        return senderThreadID;
    }

    /**
     * @param senderThreadID the senderThreadID to set
     * @since 13.12.2006
     */
    public void setSenderThreadID(long senderThreadID) {
        this.senderThreadID = senderThreadID;
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
    
    public int compareTo(Message to){
        int result=this.type.compareTo(to.type);
        if (result==0){
            result= this.priority-to.priority;
        }
        return result;
    }
    
    @Override public String toString(){
        StringBuilder str=new StringBuilder(40);
        str.append("Sender:").append(senderID).append("\n");
        str.append("Recipient:").append(recipientID).append("\n");
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
