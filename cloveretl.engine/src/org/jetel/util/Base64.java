/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-05  David Pavlis <david_pavlis@hotmail.com> and others.
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
 * Created on 7.11.2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.util;


//*********************************************************************
//* Base64 - a simple base64 encoder and decoder.
//*
//*     Copyright (c) 1999, Bob Withers - bwit@pobox.com
//*
//* This code may be freely used for any purpose, either personal
//* or commercial, provided the authors copyright notice remains
//* intact.
//*********************************************************************


public class Base64
{
    public static String encode(String data)
    {
        return(getString(encode(getBinaryBytes(data))));
    }
    
    public static byte[] encode(byte[] data)
    {
        int c;
        int len = data.length;
        StringBuffer ret = new StringBuffer(((len / 3) + 1) * 4);
        for (int i = 0; i < len; ++i)
        {
            c = (data[i] >> 2) & 0x3f;
            ret.append(cvt.charAt(c));
            c = (data[i] << 4) & 0x3f;
            if (++i < len)
                c |= (data[i] >> 4) & 0x0f;
            
            ret.append(cvt.charAt(c));
            if (i < len)
            {
                c = (data[i] << 2) & 0x3f;
                if (++i < len)
                    c |= (data[i] >> 6) & 0x03;
                
                ret.append(cvt.charAt(c));
            }
            else
            {
                ++i;
                ret.append((char) fillchar);
            }
            
            if (i < len)
            {
                c = data[i] & 0x3f;
                ret.append(cvt.charAt(c));
            }
            else
            {
                ret.append((char) fillchar);
            }
        }
        
        return(getBinaryBytes(ret.toString()));
    }
    
    public static String decode(String data)
    {
        return(getString(decode(getBinaryBytes(data))));
    }
    
    public static byte[] decode(byte[] data)
    {
        int c;
        int c1;
        int len = data.length;
        StringBuffer ret = new StringBuffer((len * 3) / 4);
        for (int i = 0; i < len; ++i)
        {
            c = cvt.indexOf(data[i]);
            ++i;
            c1 = cvt.indexOf(data[i]);
            c = ((c << 2) | ((c1 >> 4) & 0x3));
            ret.append((char) c);
            if (++i < len)
            {
                c = data[i];
                if (fillchar == c)
                    break;
                
                c = cvt.indexOf((char) c);
                c1 = ((c1 << 4) & 0xf0) | ((c >> 2) & 0xf);
                ret.append((char) c1);
            }
            
            if (++i < len)
            {
                c1 = data[i];
                if (fillchar == c1)
                    break;
                
                c1 = cvt.indexOf((char) c1);
                c = ((c << 6) & 0xc0) | c1;
                ret.append((char) c);
            }
        }
        
        return(getBinaryBytes(ret.toString()));
    }
    
    private static String getString(byte[] arr)
    {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < arr.length; ++i)
            buf.append((char) arr[i]);
        
        return(buf.toString());
    }
    
    private static byte[] getBinaryBytes(String str)
    {
        byte[] b = new byte[str.length()];
        for (int i = 0; i < b.length; ++i)
            b[i] = (byte) str.charAt(i);
        
        return(b);
    }
    
    private static final int    fillchar = '=';
    
    // 00000000001111111111222222
    // 01234567890123456789012345
    private static final String cvt = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        
        // 22223333333333444444444455
        // 67890123456789012345678901
        + "abcdefghijklmnopqrstuvwxyz"
        
        // 555555556666
        // 234567890123
        + "0123456789+/";
    
}
