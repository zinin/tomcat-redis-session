/*
 * Copyright 2011 Alexander V. Zinin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.zinin.redis.util;

import java.io.*;

/**
 * Date: 29.10.11 16:50
 *
 * @author Alexander V. Zinin (mail@zinin.ru)
 */
public class RedisSerializationUtil {
    public static <T extends Serializable> byte[] encode(T obj) {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bout);
            out.writeObject(obj);
            return bout.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error serializing object" + obj + " => " + e);
        }
    }

    @SuppressWarnings({"unchecked", "ThrowFromFinallyBlock"})
    public static <T extends Serializable> T decode(byte[] bytes) {
       return decode(bytes, null);
    }

    @SuppressWarnings({"unchecked", "ThrowFromFinallyBlock"})
    public static <T extends Serializable> T decode(byte[] bytes, ClassLoader classLoader) {
        if (classLoader == null) {
            classLoader = Thread.currentThread().getContextClassLoader();
        }
        T t = null;
        Exception thrown = null;
        try {
            CustomObjectInputStream oin = new CustomObjectInputStream(new ByteArrayInputStream(bytes), classLoader);
            t = (T) oin.readObject();
        } catch (IOException e) {
            e.printStackTrace();
            thrown = e;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            thrown = e;
        } catch (ClassCastException e) {
            e.printStackTrace();
            thrown = e;
        } finally {
            if (null != thrown) {
                throw new RuntimeException(
                        "Error decoding byte[] data to instantiate java object - " +
                                "data at key may not have been of this type or even an object", thrown
                );
            }
        }
        return t;
    }
}
