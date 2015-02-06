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

package ru.zinin.redis.session;

/**
 * Date: 28.10.11 23:58
 *
 * @author Alexander V. Zinin (mail@zinin.ru)
 */
public class RedisSessionKeys {
    private static final String SESSIONS_LIST_KEY = "sessions";

    private static final String SESSION_KEY_PREFIX = "session";

    private static final String KEY_DELIM = ":";

    private static final String SESSION_CREATION_TIME_KEY = "creation_time";
    private static final String SESSION_LAST_ACCESS_TIME_KEY = "last_access_time";
    private static final String SESSION_EXPIRE_AT_KEY = "expire_at";
    private static final String SESSION_TIMEOUT_KEY = "timeout";

    private static final String SESSION_ATTRS_LIST_KEY = "attrs";
    private static final String SESSION_ATTR_KEY = "attrs";

    private static final String SESSION_CHANNEL = "session-channel";

    private static final String ENCODING = "utf-8";

    public static String getSessionsKey() {
        return SESSIONS_LIST_KEY;
    }

    public static String getCreationTimeKey(String id) {
        return SESSION_KEY_PREFIX + KEY_DELIM + id + KEY_DELIM + SESSION_CREATION_TIME_KEY;
    }

    public static String getLastAccessTimeKey(String id) {
        return SESSION_KEY_PREFIX + KEY_DELIM + id + KEY_DELIM + SESSION_LAST_ACCESS_TIME_KEY;
    }

    public static String getExpireAtKey(String id) {
        return SESSION_KEY_PREFIX + KEY_DELIM + id + KEY_DELIM + SESSION_EXPIRE_AT_KEY;
    }

    public static String getSessionTimeoutKey(String id) {
        return SESSION_KEY_PREFIX + KEY_DELIM + id + KEY_DELIM + SESSION_TIMEOUT_KEY;
    }

    public static String getAttrsKey(String id) {
        return SESSION_KEY_PREFIX + KEY_DELIM + id + KEY_DELIM + SESSION_ATTRS_LIST_KEY;
    }

    public static String getAttrKey(String id, String name) {
        return SESSION_KEY_PREFIX + KEY_DELIM + id + KEY_DELIM + SESSION_ATTR_KEY + KEY_DELIM + name;
    }

    public static String getSessionChannel(String containerName) {
        return SESSION_CHANNEL + KEY_DELIM + containerName;
    }

    public static String getEncoding() {
        return ENCODING;
    }
}
