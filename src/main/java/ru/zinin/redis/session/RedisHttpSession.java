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

import org.apache.catalina.Context;
import org.apache.catalina.Manager;
import org.apache.catalina.Session;
import org.apache.catalina.SessionListener;
import org.apache.catalina.util.Enumerator;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import ru.zinin.redis.session.event.*;
import ru.zinin.redis.util.Base64Util;
import ru.zinin.redis.util.RedisSerializationUtil;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionContext;
import java.beans.PropertyChangeSupport;
import java.io.Serializable;
import java.security.Principal;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Date: 28.10.11 21:44
 *
 * @author Alexander V. Zinin (mail@zinin.ru)
 */
public class RedisHttpSession implements HttpSession, Session, Serializable {
    private final Log log = LogFactory.getLog(RedisHttpSession.class);

    private static final String info = "RedisSession/1.0";

    private String id;

    private RedisManager manager;
    private JedisPool pool;
    private ServletContext servletContext;
    private boolean disableListeners = false;

    private String authType;
    private Principal principal;
    private Map<String, Object> notes = new Hashtable<String, Object>();

    private AtomicBoolean isNew = new AtomicBoolean(false);

    private PropertyChangeSupport support = new PropertyChangeSupport(this);

    RedisHttpSession(String id, RedisManager manager) {
        log.trace("Create session [OLD]");

        this.id = id;
        setManager(manager);
    }

    RedisHttpSession(String id, JedisPool pool, ServletContext servletContext, boolean disableListeners) {
        log.trace("Create session [OLD] from RedisSessionTemplate.");

        this.id = id;
        this.pool = pool;
        this.servletContext = servletContext;
        this.disableListeners = disableListeners;
    }

    RedisHttpSession(String id, RedisManager manager, int maxInactiveInterval) {
        log.trace("Create session [NEW]. maxInactiveInterval = " + maxInactiveInterval);

        this.id = id;
        setManager(manager);

        isNew.set(true);

        String sessionsKey = RedisSessionKeys.getSessionsKey();
        String creationTimeKey = RedisSessionKeys.getCreationTimeKey(id);
        String lastAccessTimeKey = RedisSessionKeys.getLastAccessTimeKey(id);
        String expiresAtKey = RedisSessionKeys.getExpireAtKey(id);
        String timeoutKey = RedisSessionKeys.getSessionTimeoutKey(id);

        long currentTime = System.currentTimeMillis();
        long expireAtTime = currentTime + (maxInactiveInterval * 1000);
        long expireAtTimeWithReserve = currentTime + (maxInactiveInterval * 1000 * 2);

        Jedis jedis = pool.getResource();
        try {
            Transaction transaction = jedis.multi();

            transaction.set(creationTimeKey, Long.toString(currentTime));
            transaction.set(lastAccessTimeKey, Long.toString(currentTime));
            transaction.set(expiresAtKey, Long.toString(expireAtTimeWithReserve));
            transaction.set(timeoutKey, Integer.toString(maxInactiveInterval));

            transaction.expireAt(creationTimeKey, getUnixTime(expireAtTimeWithReserve));
            transaction.expireAt(lastAccessTimeKey, getUnixTime(expireAtTime));
            transaction.expireAt(expiresAtKey, getUnixTime(expireAtTimeWithReserve));
            transaction.expireAt(timeoutKey, getUnixTime(expireAtTimeWithReserve));

            transaction.zadd(sessionsKey, currentTime, id);

            transaction.exec();

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }

        tellNew();
    }

    private long getUnixTime(long time) {
        return time / 1000;
    }

    public void tellNew() {
        if (!disableListeners) {
            Jedis jedis = pool.getResource();
            try {
                RedisSessionEvent redisSessionEvent = new RedisSessionCreatedEvent(id);
                byte[] bytes = RedisSerializationUtil.encode(redisSessionEvent);
                String message = new String(Base64Util.encode(bytes));

                jedis.publish(RedisSessionKeys.getSessionChannel(manager.getContainer().getName()), message);

                log.debug("Sended " + redisSessionEvent.toString());

                pool.returnResource(jedis);
            } catch (Throwable e) {
                pool.returnBrokenResource(jedis);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public String getAuthType() {
        log.trace("EXEC getAuthType();");

        return authType;
    }

    @Override
    public void setAuthType(String authType) {
        log.trace(String.format("EXEC setAuthType(%s);", authType));

        String oldAuthType = this.authType;
        this.authType = authType;

        support.firePropertyChange("authType", oldAuthType, this.authType);
    }

    @Override
    public long getCreationTime() {
        log.trace("EXEC getCreationTime();");

        String key = RedisSessionKeys.getCreationTimeKey(id);

        String creationTime;
        Jedis jedis = pool.getResource();
        try {
            creationTime = jedis.get(key);

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }

        if (creationTime == null) {
            throw new IllegalStateException("Can't get creation time from redis.");
        }

        return Long.parseLong(creationTime);
    }

    @Override
    public long getCreationTimeInternal() {
        log.trace("EXEC getCreationTimeInternal();");

        return getCreationTime();
    }

    @Override
    public void setCreationTime(long time) {
        log.trace(String.format("EXEC setAuthType(%d);", time));

        throw new UnsupportedOperationException("Can't set creation time");
    }

    @Override
    public String getId() {
        log.trace("EXEC getId();");

        return id;
    }

    @Override
    public String getIdInternal() {
        log.trace("EXEC getIdInternal();");

        return id;
    }

    @Override
    public void setId(String id) {
        log.trace(String.format("EXEC setId(%s);", id));

        setId(id, false);
    }

    @Override
    public void setId(String id, boolean notify) {
        log.trace(String.format("EXEC setId(%s, %s);", id, notify));

        String oldCreationTimeKey = RedisSessionKeys.getCreationTimeKey(this.id);
        String oldLastAccessTimeKey = RedisSessionKeys.getLastAccessTimeKey(this.id);
        String oldExpiresAtKey = RedisSessionKeys.getExpireAtKey(this.id);
        String oldTimeoutKey = RedisSessionKeys.getSessionTimeoutKey(this.id);
        String oldAttrsKey = RedisSessionKeys.getAttrsKey(this.id);

        String newCreationTimeKey = RedisSessionKeys.getCreationTimeKey(id);
        String newLastAccessTimeKey = RedisSessionKeys.getLastAccessTimeKey(id);
        String newExpiresAtKey = RedisSessionKeys.getExpireAtKey(id);
        String newTimeoutKey = RedisSessionKeys.getSessionTimeoutKey(id);
        String newAttrsKey = RedisSessionKeys.getAttrsKey(id);

        Set<String> attributeNames = getAttributesNames();

        long lastAccessTime = getLastAccessedTime();

        Jedis jedis = pool.getResource();
        try {
            Transaction transaction = jedis.multi();

            transaction.rename(oldCreationTimeKey, newCreationTimeKey);
            transaction.rename(oldLastAccessTimeKey, newLastAccessTimeKey);
            transaction.rename(oldExpiresAtKey, newExpiresAtKey);
            transaction.rename(oldTimeoutKey, newTimeoutKey);

            if (attributeNames != null && !attributeNames.isEmpty()) {
                for (String attributeName : attributeNames) {
                    String oldKey = RedisSessionKeys.getAttrKey(this.id, attributeName);
                    String newKey = RedisSessionKeys.getAttrKey(id, attributeName);
                    transaction.rename(oldKey, newKey);
                }

                transaction.rename(oldAttrsKey, newAttrsKey);
            } else {
                transaction.del(oldAttrsKey);
            }

            transaction.zadd(RedisSessionKeys.getSessionsKey(), lastAccessTime, id);
            transaction.zrem(RedisSessionKeys.getSessionsKey(), this.id);

            transaction.exec();

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }

        this.id = id;

        if (notify) {
            tellNew();
        }
    }

    @Override
    public String getInfo() {
        log.trace("EXEC getInfo();");

        return info;
    }

    private Long getLastAccessTime() {
        String key = RedisSessionKeys.getLastAccessTimeKey(id);

        String lastAccessTime;
        Jedis jedis = pool.getResource();
        try {
            lastAccessTime = jedis.get(key);

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }

        if (lastAccessTime == null) {
            return null;
        }

        return Long.parseLong(lastAccessTime);
    }

    @Override
    public long getThisAccessedTime() {
        log.trace("EXEC getThisAccessedTime();");

        return getLastAccessedTime();
    }

    @Override
    public long getThisAccessedTimeInternal() {
        log.trace("EXEC getThisAccessedTimeInternal();");

        return getLastAccessedTime();
    }

    @Override
    public long getLastAccessedTime() {
        log.trace("EXEC getLastAccessedTime();");

        Long lastAccessTime = getLastAccessTime();

        if (lastAccessTime == null) {
            throw new IllegalStateException("Can't get last access time from redis.");
        }

        return lastAccessTime;
    }

    @Override
    public long getLastAccessedTimeInternal() {
        log.trace("EXEC getLastAccessedTimeInternal();");

        return getLastAccessedTime();
    }

    @Override
    public Manager getManager() {
        log.trace("EXEC getManager();");

        return manager;
    }

    @Override
    public void setManager(Manager manager) {
        log.trace(String.format("EXEC setId(%s);", manager));

        this.manager = (RedisManager) manager;
        this.pool = this.manager.getPool();
        this.servletContext = ((Context) manager.getContainer()).getServletContext();
        this.disableListeners = this.manager.isDisableListeners();
    }

    @Override
    public ServletContext getServletContext() {
        log.trace("EXEC getServletContext();");

        return servletContext;
    }

    @Override
    public void setMaxInactiveInterval(int interval) {
        log.trace(String.format("EXEC setMaxInactiveInterval(%d);", interval));

        String key = RedisSessionKeys.getSessionTimeoutKey(id);

        Jedis jedis = pool.getResource();
        try {
            jedis.set(key, Integer.toString(interval));

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }

        renewAll();
    }

    @Override
    public void setNew(boolean isNew) {
        log.trace(String.format("EXEC setNew(%s);", isNew));

        throw new UnsupportedOperationException("Can't set new");
    }

    @Override
    public Principal getPrincipal() {
        log.trace("EXEC getPrincipal();");

        return principal;
    }

    @Override
    public void setPrincipal(Principal principal) {
        log.trace(String.format("EXEC setPrincipal(%s);", principal));

        Principal oldPrincipal = this.principal;
        this.principal = principal;

        support.firePropertyChange("principal", oldPrincipal, this.principal);
    }

    @Override
    public HttpSession getSession() {
        log.trace("EXEC getSession();");

        return this;
    }

    @Override
    public void setValid(boolean isValid) {
        log.trace(String.format("EXEC setValid(%s);", isValid));

        throw new UnsupportedOperationException("Can't set valid.");
    }

    @Override
    public boolean isValid() {
        log.trace("EXEC isValid();");

        return !isAnyRequiredFieldNull();
    }

    private boolean isAnyRequiredFieldNull() {
        try {
            getLastAccessedTime();
            getExpireAt();
            getMaxInactiveInterval();
        } catch (IllegalStateException e) {
            return true;
        }

        return false;
    }

    private void renewAll() {
        String creationTimeKey = RedisSessionKeys.getCreationTimeKey(id);
        String lastAccessTimeKey = RedisSessionKeys.getLastAccessTimeKey(id);
        String expiresAtKey = RedisSessionKeys.getExpireAtKey(id);
        String timeoutKey = RedisSessionKeys.getSessionTimeoutKey(id);
        String attrsKey = RedisSessionKeys.getAttrsKey(id);

        Set<String> attributeNames = null;

        long currentExpireAtTime = getExpireAt();
        long timeout = getMaxInactiveInterval();

        long currentTime = System.currentTimeMillis();
        long expireAtTime = currentTime + (timeout * 1000);
        long expireAtTimeWithReserve = currentTime + (timeout * 1000 * 2);

        if (currentExpireAtTime < expireAtTime) {
            attributeNames = getAttributesNames();
        }

        Jedis jedis = pool.getResource();
        try {
            Transaction transaction = jedis.multi();

            if (currentExpireAtTime < expireAtTime) {
                transaction.set(expiresAtKey, Long.toString(expireAtTimeWithReserve));
                transaction.expireAt(expiresAtKey, getUnixTime(expireAtTimeWithReserve));

                transaction.expireAt(creationTimeKey, getUnixTime(expireAtTimeWithReserve));
                transaction.expireAt(timeoutKey, getUnixTime(expireAtTimeWithReserve));

                if (attributeNames != null && !attributeNames.isEmpty()) {
                    for (String attributeName : attributeNames) {
                        String key = RedisSessionKeys.getAttrKey(id, attributeName);
                        transaction.expireAt(key, getUnixTime(expireAtTimeWithReserve));
                    }

                    transaction.expireAt(attrsKey, getUnixTime(expireAtTimeWithReserve));
                }
            }

            transaction.set(lastAccessTimeKey, Long.toString(currentTime));
            transaction.expireAt(lastAccessTimeKey, getUnixTime(expireAtTime));

            transaction.zadd(RedisSessionKeys.getSessionsKey(), currentTime, id);

            transaction.exec();

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void access() {
        log.trace("EXEC access();");

        renewAll();
    }

    @Override
    public void addSessionListener(SessionListener listener) {
        log.trace(String.format("EXEC addSessionListener(%s);", listener));

        throw new UnsupportedOperationException("Listeners are not supported.");
    }

    @Override
    public void endAccess() {
        log.trace("EXEC endAccess();");

        isNew.set(false);
    }

    @Override
    public void expire() {
        log.trace("EXEC expire();");

        String creationTimeKey = RedisSessionKeys.getCreationTimeKey(id);
        String lastAccessTimeKey = RedisSessionKeys.getLastAccessTimeKey(id);
        String expiresAtKey = RedisSessionKeys.getExpireAtKey(id);
        String timeoutKey = RedisSessionKeys.getSessionTimeoutKey(id);
        String attrsKey = RedisSessionKeys.getAttrsKey(id);

        Set<String> attributeNames = getAttributesNames();

        Jedis jedis = pool.getResource();
        try {
            Transaction transaction = jedis.multi();

            transaction.del(creationTimeKey, lastAccessTimeKey, expiresAtKey, timeoutKey, attrsKey);

            if (!attributeNames.isEmpty()) {
                Set<String> keys = new HashSet<String>();
                for (String attributeName : attributeNames) {
                    String key = RedisSessionKeys.getAttrKey(id, attributeName);
                    keys.add(key);
                }

                //noinspection ToArrayCallWithZeroLengthArrayArgument
                transaction.del(keys.toArray(new String[]{}));
            }

            if (!disableListeners) {
                RedisSessionEvent redisSessionEvent = new RedisSessionDestroyedEvent(id);
                byte[] bytes = RedisSerializationUtil.encode(redisSessionEvent);
                String message = new String(Base64Util.encode(bytes));

                transaction.publish(RedisSessionKeys.getSessionChannel(manager.getContainer().getName()), message);
            }

            transaction.exec();

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object getNote(String name) {
        log.trace(String.format("EXEC getNote(%s);", name));

        return notes.get(name);
    }

    @Override
    public Iterator<String> getNoteNames() {
        log.trace("EXEC getNoteNames();");

        return notes.keySet().iterator();
    }

    @Override
    public void recycle() {
        log.trace("EXEC recycle();");

        throw new IllegalStateException("Recycle is not supported");
    }

    @Override
    public void removeNote(String name) {
        log.trace(String.format("EXEC removeNote(%s);", name));

        notes.remove(name);
    }

    @Override
    public void removeSessionListener(SessionListener listener) {
        log.trace(String.format("EXEC removeSessionListener(%s);", listener));

        throw new UnsupportedOperationException("Listeners are not supported.");
    }

    @Override
    public void setNote(String name, Object value) {
        log.trace(String.format("EXEC setNote(%s, %s);", name, value));

        notes.put(name, value);
    }

    @Override
    public int getMaxInactiveInterval() {
        log.trace("EXEC getMaxInactiveInterval();");

        String key = RedisSessionKeys.getSessionTimeoutKey(id);

        String sessionTimeout;
        Jedis jedis = pool.getResource();
        try {
            sessionTimeout = jedis.get(key);

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }

        if (sessionTimeout == null) {
            throw new IllegalStateException("Can't get session timeout from redis.");
        }

        return Integer.parseInt(sessionTimeout);
    }

    @SuppressWarnings({"deprecation"})
    @Override
    public HttpSessionContext getSessionContext() {
        log.trace("EXEC getSessionContext();");

        return null;
    }

    @Override
    public Object getAttribute(String name) {
        log.trace(String.format("EXEC getAttribute(%s);", name));

        String key = RedisSessionKeys.getAttrKey(id, name);

        byte[] object;
        Jedis jedis = pool.getResource();
        try {
            object = jedis.get(key.getBytes(RedisSessionKeys.getEncoding()));

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }

        if (object == null) {
            return null;
        }

        return RedisSerializationUtil.decode(object);
    }

    @Override
    public Object getValue(String name) {
        log.trace(String.format("EXEC getValue(%s);", name));

        return getAttribute(name);
    }

    private Set<String> getAttributesNames() {
        String key = RedisSessionKeys.getAttrsKey(id);

        Set<String> result;
        Jedis jedis = pool.getResource();
        try {
            result = jedis.smembers(key);

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }

        if (result.isEmpty()) {
            return new HashSet<String>();
        }

        return result;
    }

    public long getExpireAt() {
        String key = RedisSessionKeys.getExpireAtKey(id);

        String expireAt;
        Jedis jedis = pool.getResource();
        try {
            expireAt = jedis.get(key);

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }

        if (expireAt == null) {
            throw new IllegalStateException("Can't get expireAt from redis.");
        }

        return Long.parseLong(expireAt);
    }

    @Override
    public Enumeration<String> getAttributeNames() {
        log.trace("EXEC getAttributeNames();");

        return new Enumerator<String>(getAttributesNames(), true);
    }

    @Override
    public String[] getValueNames() {
        log.trace("EXEC getValueNames();");

        //noinspection ToArrayCallWithZeroLengthArrayArgument
        return getAttributesNames().toArray(new String[]{});
    }

    @Override
    public void setAttribute(String name, Object value) {
        log.trace(String.format("EXEC setAttribute(%s, %s);", name, value));

        String attributeKey = RedisSessionKeys.getAttrKey(id, name);
        String attrsListKey = RedisSessionKeys.getAttrsKey(id);

        Long currentExpireAtTimeWithReserve = getExpireAt();

        boolean exist;
        Jedis jedis = pool.getResource();
        try {
            exist = jedis.exists(attributeKey);

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }

        byte[] bytes = null;
        if (exist && !disableListeners) {
            jedis = pool.getResource();
            try {
                bytes = jedis.get(attributeKey.getBytes(RedisSessionKeys.getEncoding()));

                pool.returnResource(jedis);
            } catch (Throwable e) {
                pool.returnBrokenResource(jedis);
                throw new RuntimeException(e);
            }
        }

        jedis = pool.getResource();
        try {
            Transaction transaction = jedis.multi();

            byte[] object = RedisSerializationUtil.encode((Serializable) value);
            transaction.set(attributeKey.getBytes(RedisSessionKeys.getEncoding()), object);
            transaction.expireAt(attributeKey.getBytes(RedisSessionKeys.getEncoding()), getUnixTime(currentExpireAtTimeWithReserve));

            transaction.sadd(attrsListKey, name);
            transaction.expireAt(attrsListKey, getUnixTime(currentExpireAtTimeWithReserve));

            transaction.exec();

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }

        if (!disableListeners) {
            RedisSessionEvent sessionEvent;
            if (bytes == null) {
                sessionEvent = new RedisSessionAddAttributeEvent(id, name, (Serializable) value);
            } else {
                Object object = RedisSerializationUtil.decode(bytes);
                sessionEvent = new RedisSessionReplaceAttributeEvent(id, name, (Serializable) object);
            }

            jedis = pool.getResource();
            try {
                bytes = RedisSerializationUtil.encode(sessionEvent);
                String message = new String(Base64Util.encode(bytes));

                jedis.publish(RedisSessionKeys.getSessionChannel(manager.getContainer().getName()), message);

                log.debug("Sended " + sessionEvent.toString());

                pool.returnResource(jedis);
            } catch (Throwable e) {
                pool.returnBrokenResource(jedis);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void putValue(String name, Object value) {
        log.trace(String.format("EXEC putValue(%s, %s);", name, value));

        setAttribute(name, value);
    }

    @Override
    public void removeAttribute(String name) {
        log.trace(String.format("EXEC removeAttribute(%s);", name));

        String attributeKey = RedisSessionKeys.getAttrKey(id, name);
        String attrsListKey = RedisSessionKeys.getAttrsKey(id);

        boolean exist;
        Jedis jedis = pool.getResource();
        try {
            exist = jedis.exists(attributeKey);

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }

        if (!exist) {
            return;
        }

        String message = null;
        if (!disableListeners) {
            byte[] bytes;

            jedis = pool.getResource();
            try {
                bytes = jedis.get(attributeKey.getBytes(RedisSessionKeys.getEncoding()));

                pool.returnResource(jedis);
            } catch (Throwable e) {
                pool.returnBrokenResource(jedis);
                throw new RuntimeException(e);
            }

            Object object = RedisSerializationUtil.decode(bytes);
            RedisSessionEvent sessionEvent = new RedisSessionRemoveAttributeEvent(id, name, (Serializable) object);

            bytes = RedisSerializationUtil.encode(sessionEvent);
            message = new String(Base64Util.encode(bytes));
        }

        jedis = pool.getResource();
        try {
            Transaction transaction = jedis.multi();

            transaction.del(attributeKey.getBytes(RedisSessionKeys.getEncoding()));

            transaction.srem(attrsListKey, name);

            if (!disableListeners) {
                transaction.publish(RedisSessionKeys.getSessionChannel(manager.getContainer().getName()), message);
            }

            transaction.exec();

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeValue(String name) {
        log.trace(String.format("EXEC removeValue(%s);", name));

        removeAttribute(name);
    }

    @Override
    public void invalidate() {
        log.trace("EXEC invalidate();");

        expire();
    }

    @Override
    public boolean isNew() {
        log.trace("EXEC isNew();");

        return isNew.get();
    }
}
