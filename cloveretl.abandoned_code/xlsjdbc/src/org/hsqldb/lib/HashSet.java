/* Copyright (c) 2001-2004, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG, 
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb.lib;

import org.hsqldb.store.BaseHashMap;

/**
 * This class does not store null keys.
 *
 * @author fredt@users
 * @version 1.7.2
 * @since 1.7.2
 */
public class HashSet extends BaseHashMap implements Set {

    public HashSet() {
        this(16, 0.75f);
    }

    public HashSet(int initialCapacity) throws IllegalArgumentException {
        this(initialCapacity, 0.75f);
    }

    public HashSet(int initialCapacity,
                   float loadFactor) throws IllegalArgumentException {
        super(initialCapacity, loadFactor, BaseHashMap.objectKeyOrValue,
              BaseHashMap.noKeyOrValue, false);
    }

    public boolean contains(Object key) {
        return super.containsKey(key);
    }

    public Object get(Object key) {

        int lookup = getLookup(key, key.hashCode());

        if (lookup < 0) {
            return null;
        } else {
            return objectKeyTable[lookup];
        }
    }

    public boolean add(Object key) {

        int oldSize = size();

        super.addOrRemove(0, 0, key, null, false);

        return oldSize != size();
    }

    public boolean addAll(Object[] keys) {

        boolean changed = false;

        for (int i = 0; i < keys.length; i++) {
            if (add(keys[i])) {
                changed = true;
            }
        }

        return changed;
    }

    public boolean remove(Object key) {

        int oldSize = size();

        super.removeObject(key);

        return oldSize != size();
    }

    public Object[] toArray(Object a[]) {

        if (a == null || a.length < size()) {
            a = new Object[size()];
        }

        Iterator it = iterator();

        for (int i = 0; it.hasNext(); i++) {
            a[i] = it.next();
        }

        return a;
    }

    public Iterator iterator() {
        return new BaseHashIterator(true);
    }
}
