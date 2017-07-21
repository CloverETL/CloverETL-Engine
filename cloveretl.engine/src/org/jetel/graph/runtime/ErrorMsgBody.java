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


public class ErrorMsgBody {
    
    protected int errorCode;
    protected String errorMessage;
    protected Throwable sourceException;
    
    /**
     * @param node
     * @param errorCode
     * @param errorMessage
     * @param exception
     */
    public ErrorMsgBody(int errorCode,String errorMessage,Throwable exception){
        this.errorCode=errorCode;
        this.errorMessage=errorMessage;
        this.sourceException=exception;
    }

    /**
     * @return the errorCode
     * @since 13.12.2006
     */
    public int getErrorCode() {
        return errorCode;
    }

    /**
     * @param errorCode the errorCode to set
     * @since 13.12.2006
     */
    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    /**
     * @return the errorMessage
     * @since 13.12.2006
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * @param errorMessage the errorMessage to set
     * @since 13.12.2006
     */
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * @return the exception
     * @since 13.12.2006
     */
    public Throwable getSourceException() {
        return sourceException;
    }

    /**
     * @param exception the exception to set
     * @since 13.12.2006
     */
    public void setSourceException(Throwable exception) {
        this.sourceException = exception;
    }
    
    @Override public String toString(){
        return errorMessage+" : "+sourceException.toString();
    }

}
