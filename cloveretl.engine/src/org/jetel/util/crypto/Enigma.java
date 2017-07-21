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
package org.jetel.util.crypto;

import java.security.AlgorithmParameters;
import java.security.spec.AlgorithmParameterSpec;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;

import org.jetel.exception.JetelException;

/**
 * Class for encrypting/decrypting strings (mostly passwords) stored by CloverETL
 * 
 * @author Martin Zatopek, JavlinConsulting,s.r.o.
 * @since  16.8.2006
 *
 */
public class Enigma {
    
    private boolean initialized = false;
    
    private SecretKey key;
    private Object algorithmParameters;
    private Cipher cipher;
    
    public Enigma(String password) {
        //by default is used the EncryptDESWithMD5 class as encrypt/decrypt algorithm
        EncryptAlgorithm encrypter = new EncryptDESWithMD5();
        key = encrypter.createSecretKey(password);
        algorithmParameters = encrypter.createAlgorithmParameters();
        cipher = encrypter.createCipher();
        
        if(key != null && cipher != null) {
            initialized = true;
        }
    }
    
    /**
     * @param input
     * @return
     * @throws JetelException if enigma is not valid initialized
     */
    public synchronized String encrypt(String input) throws JetelException {
        if(!initialized) {
            throw new JetelException("Enigma is not valid initialized.");
        }
        
        initCipher(Cipher.ENCRYPT_MODE);
        try {
            byte[] out = cipher.doFinal(input.getBytes());
            return Base64.encodeBytes(out);
        } catch (Throwable e) {
            throw new JetelException("Enigma is not valid initialized.", e);
        }
    }

    public synchronized String decrypt(String input) throws JetelException {
        if(!initialized) {
            throw new JetelException("Enigma is not valid initialized.");
        }
        
        initCipher(Cipher.DECRYPT_MODE);
        try {
            byte[] out = Base64.decode(input);
            out = cipher.doFinal(out);
            return new String(out);
        } catch (Throwable e) {
            throw new JetelException("Enigma is not valid initialized.", e);
        }
    }

    private void initCipher(int mode) throws JetelException {
        try {
            if(algorithmParameters == null) {
                cipher.init(mode, key);
            } else {
                if(algorithmParameters instanceof AlgorithmParameters) {
                    cipher.init(mode, key, (AlgorithmParameters) algorithmParameters);
                } else if(algorithmParameters instanceof AlgorithmParameterSpec) {
                    cipher.init(mode, key, (AlgorithmParameterSpec) algorithmParameters);
                } else {
                    throw new JetelException("Algorithm parameters are not valid class.");
                }
            }
        } catch (Throwable e) {
            throw new JetelException("Enigma is not valid initialized.", e);
        }
    }
    
    public static void main(String[] args) {
        if(args.length != 2) {
            System.out.println("Usage: org.jetel.util.Enigma <password> <text_to_encrypt>");
            return;
        }
        String password = args[0];
        String text = args[1];
        Enigma enigma = new Enigma(password);
        
        try {
            System.out.println(enigma.encrypt(text));
        } catch (JetelException e) {
            e.printStackTrace();
        }
        
    }
}
