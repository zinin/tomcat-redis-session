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

import org.apache.catalina.*;
import org.apache.catalina.session.Constants;
import org.apache.catalina.session.ManagerBase;
import org.apache.catalina.util.SessionIdGeneratorBase;
import org.apache.catalina.util.StandardSessionIdGenerator;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.res.StringManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;

/**
 * Date: 28.10.11 22:16
 *
 * @author Alexander V. Zinin (mail@zinin.ru)
 */
public class RedisManager extends ManagerBase implements Manager, PropertyChangeListener {
    private final Log log = LogFactory.getLog(RedisManager.class);

    private static final StringManager sm = StringManager.getManager(Constants.Package);

    private int maxInactiveInterval = 30 * 60;
    private int sessionIdLength = 32;
    private int maxActiveSessions = -1;

    private String secureRandomClass = null;
    private String secureRandomAlgorithm = "SHA1PRNG";
    private String secureRandomProvider = null;
    private SessionIdGenerator sessionIdGenerator = null;

    private String jedisJndiName = "pool/jedis";

    private String redisHostname = "localhost";
    private int redisPort = Protocol.DEFAULT_PORT;
    private int redisTimeout = Protocol.DEFAULT_TIMEOUT;
    private String redisPassword;
    private JedisPool pool;

    private boolean disableListeners = false;

    private PropertyChangeSupport support = new PropertyChangeSupport(this);

    private final RedisEventListenerThread eventListenerThread = new RedisEventListenerThread(this);

    @Override
    public boolean getDistributable() {
        log.trace("EXEC getDistributable();");

        return true;
    }

    @Override
    public void setDistributable(boolean distributable) {
        log.trace(String.format("EXEC setDistributable(%s);", distributable));

        if (!distributable) {
            log.error("Only distributable web applications supported.");
        }
    }

    @Override
    public int getMaxInactiveInterval() {
        log.trace("EXEC getMaxInactiveInterval();");

        return maxInactiveInterval;
    }

    @Override
    public void setMaxInactiveInterval(int interval) {
        log.trace(String.format("EXEC setMaxInactiveInterval(%d);", interval));

        int oldMaxInactiveInterval = this.maxInactiveInterval;
        this.maxInactiveInterval = interval;
        support.firePropertyChange("maxInactiveInterval", Integer.valueOf(oldMaxInactiveInterval), Integer.valueOf(this.maxInactiveInterval));
    }

    @Override
    public long getSessionCounter() {
        log.trace("EXEC getSessionCounter();");

        return 0;
    }

    @Override
    public void setSessionCounter(long sessionCounter) {
        log.trace(String.format("EXEC setSessionCounter(%d);", sessionCounter));
    }

    @Override
    public int getMaxActive() {
        log.trace("EXEC getMaxActive();");

        return 0;
    }

    @Override
    public void setMaxActive(int maxActive) {
        log.trace(String.format("EXEC setMaxActive(%d);", maxActive));
    }

    public int getMaxActiveSessions() {
        log.trace("EXEC getMaxActiveSessions();");

        return this.maxActiveSessions;
    }

    public void setMaxActiveSessions(int max) {
        log.trace(String.format("EXEC setMaxActiveSessions(%d);", max));

        int oldMaxActiveSessions = this.maxActiveSessions;
        this.maxActiveSessions = max;
        support.firePropertyChange("maxActiveSessions", Integer.valueOf(oldMaxActiveSessions), Integer.valueOf(this.maxActiveSessions));
    }

    @Override
    public int getActiveSessions() {
        log.trace("EXEC getActiveSessions();");

        String key = RedisSessionKeys.getSessionsKey();

        Long result;
        Jedis jedis = pool.getResource();
        try {
            result = jedis.zcard(key);
            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }

        return result.intValue();
    }

    @Override
    public long getExpiredSessions() {
        log.trace("EXEC getExpiredSessions();");

        return 0;
    }

    @Override
    public void setExpiredSessions(long expiredSessions) {
        log.trace(String.format("EXEC setExpiredSessions(%d);", expiredSessions));
    }

    @Override
    public int getRejectedSessions() {
        log.trace("EXEC getRejectedSessions();");

        return 0;
    }

    @Override
    public int getSessionMaxAliveTime() {
        log.trace("EXEC getSessionMaxAliveTime();");

        return 0;
    }

    @Override
    public void setSessionMaxAliveTime(int sessionMaxAliveTime) {
        log.trace(String.format("EXEC setSessionMaxAliveTime(%d);", sessionMaxAliveTime));
    }

    @Override
    public int getSessionAverageAliveTime() {
        log.trace("EXEC getSessionAverageAliveTime();");

        return 0;
    }

    @Override
    public int getSessionCreateRate() {
        log.trace("EXEC getSessionCreateRate();");

        return 0;
    }

    @Override
    public int getSessionExpireRate() {
        log.trace("EXEC getSessionExpireRate();");

        return 0;
    }

    @Override
    public void add(Session session) {
        log.trace(String.format("EXEC add(%s);", session));
    }

    @Override
    public void addPropertyChangeListener(PropertyChangeListener listener) {
        log.trace(String.format("EXEC addPropertyChangeListener(%s);", listener));

        support.addPropertyChangeListener(listener);
    }

    @Override
    public Session createEmptySession() {
        log.trace("EXEC createEmptySession();");

        throw new UnsupportedOperationException("Cannot create empty session.");
    }

    @Override
    public Session createSession(String sessionId) {
        log.trace(String.format("EXEC createSession(%s);", sessionId));

        if ((maxActiveSessions >= 0) && (getActiveSessions() >= maxActiveSessions)) {
            throw new IllegalStateException(sm.getString("managerBase.createSession.ise"));
        }

        String id = sessionId;
        if (id == null) {
            id = generateSessionId();
        }

        return new RedisHttpSession(id, this, maxInactiveInterval);
    }

    @Override
    public Session findSession(String id) throws IOException {
        log.trace(String.format("EXEC findSession(%s);", id));

        if (id == null) {
            return null;
        }

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

        return new RedisHttpSession(id, this);
    }

    @Override
    public Session[] findSessions() {
        log.trace("EXEC findSessions();");

        Set<String> sessionIds;

        Jedis jedis = pool.getResource();
        try {
            sessionIds = jedis.zrangeByScore(RedisSessionKeys.getSessionsKey(), 0, Double.MAX_VALUE);

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }

        Set<RedisHttpSession> result = new HashSet<RedisHttpSession>(sessionIds.size());
        for (String sessionId : sessionIds) {
            RedisHttpSession session = new RedisHttpSession(sessionId, this);
            if (session.isValid()) {
                result.add(session);
            }
        }

        //noinspection ToArrayCallWithZeroLengthArrayArgument
        return result.toArray(new Session[]{});
    }

    @Override
    public void load() throws ClassNotFoundException, IOException {
        log.trace("EXEC load();");
    }

    @Override
    public void remove(Session session) {
        log.trace(String.format("EXEC remove(%s);", session));

        remove(session, false);
    }

    @Override
    public void remove(Session session, boolean update) {
        log.trace(String.format("EXEC remove(%s, %s);", session, update));
    }

    @Override
    public void removePropertyChangeListener(PropertyChangeListener listener) {
        log.trace(String.format("EXEC removePropertyChangeListener(%s);", listener));

        support.removePropertyChangeListener(listener);
    }

    @Override
    public void unload() throws IOException {
        log.trace("EXEC unload();");
    }

    @Override
    public void backgroundProcess() {
        log.trace("EXEC backgroundProcess();");

        long max = System.currentTimeMillis() - (maxInactiveInterval * 1000);
        Set<String> sessionIds;

        Jedis jedis = pool.getResource();
        try {
            sessionIds = jedis.zrangeByScore(RedisSessionKeys.getSessionsKey(), 0, max);

            pool.returnResource(jedis);
        } catch (Throwable e) {
            pool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }

        log.debug(sessionIds.size() + " sessions expired.");

        Set<String> removedIds = new HashSet<String>();
        if (!sessionIds.isEmpty()) {
            jedis = pool.getResource();
            try {
                for (String sessionId : sessionIds) {
                    if (jedis.zrem(RedisSessionKeys.getSessionsKey(), sessionId) > 0) {
                        removedIds.add(sessionId);
                    }
                }

                pool.returnResource(jedis);
            } catch (Throwable e) {
                pool.returnBrokenResource(jedis);
                throw new RuntimeException(e);
            }

            for (String sessionId : removedIds) {
                RedisHttpSession session = new RedisHttpSession(sessionId, this);

                session.expire();
            }
        }
    }

    public String getJedisJndiName() {
        return jedisJndiName;
    }

    public void setJedisJndiName(String jedisJndiName) {
        this.jedisJndiName = jedisJndiName;
    }

    public String getRedisHostname() {
        return redisHostname;
    }

    public void setRedisHostname(String redisHostname) {
        this.redisHostname = redisHostname;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public int getRedisTimeout() {
        return redisTimeout;
    }

    public void setRedisTimeout(int redisTimeout) {
        this.redisTimeout = redisTimeout;
    }

    public String getRedisPassword() {
        return redisPassword;
    }

    public void setRedisPassword(String redisPassword) {
        this.redisPassword = redisPassword;
    }

    public JedisPool getPool() {
        return pool;
    }

    public boolean isDisableListeners() {
        return disableListeners;
    }

    public void setDisableListeners(boolean disableListeners) {
        this.disableListeners = disableListeners;
    }

    @Override
    protected void startInternal() throws LifecycleException {
        log.trace("EXEC startInternal();");

        // Ensure caches for timing stats are the right size by filling with
        // nulls.
        while (sessionCreationTiming.size() < TIMING_STATS_CACHE_SIZE) {
            sessionCreationTiming.add(null);
        }
        while (sessionExpirationTiming.size() < TIMING_STATS_CACHE_SIZE) {
            sessionExpirationTiming.add(null);
        }

          /* Create sessionIdGenerator if not explicitly configured */
        SessionIdGenerator sessionIdGenerator = getSessionIdGenerator();
        if (sessionIdGenerator == null) {
            sessionIdGenerator = new StandardSessionIdGenerator();
            setSessionIdGenerator(sessionIdGenerator);
        }

        if (sessionIdLength != SESSION_ID_LENGTH_UNSET) {
            sessionIdGenerator.setSessionIdLength(sessionIdLength);
        }
        sessionIdGenerator.setJvmRoute(getJvmRoute());
        if (sessionIdGenerator instanceof SessionIdGeneratorBase) {
            SessionIdGeneratorBase sig = (SessionIdGeneratorBase) sessionIdGenerator;
            sig.setSecureRandomAlgorithm(getSecureRandomAlgorithm());
            sig.setSecureRandomClass(getSecureRandomClass());
            sig.setSecureRandomProvider(getSecureRandomProvider());
        }

        // Force initialization of the random number generator
        if (log.isDebugEnabled()) {
            log.debug("Force random number initialization starting");
        }
        sessionIdGenerator.generateSessionId();
        if (log.isDebugEnabled()) {
            log.debug("Force random number initialization completed");
        }

        log.debug("Trying to get jedis pool from JNDI...");
        InitialContext initialContext = null;
        try {
            initialContext = new InitialContext();
            pool = (JedisPool) initialContext.lookup("java:/comp/env/" + jedisJndiName);
        } catch (NamingException e) {
            log.warn("JedisPool not found in JNDI");
        }

        log.debug("Pool from JNDI: " + pool);

        if (pool == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            pool = new JedisPool(config, redisHostname, redisPort, redisTimeout, redisPassword);
        }

        if (!disableListeners) {
            Executors.newSingleThreadExecutor().execute(eventListenerThread);
        }

        setState(LifecycleState.STARTING);
    }

    public String getSecureRandomClass() {
        return this.secureRandomClass;
    }

    public void setSecureRandomClass(String secureRandomClass) {
        String oldSecureRandomClass = this.secureRandomClass;
        this.secureRandomClass = secureRandomClass;
        support.firePropertyChange("secureRandomClass", oldSecureRandomClass, this.secureRandomClass);
    }

    public String getSecureRandomAlgorithm() {
        return secureRandomAlgorithm;
    }

    public void setSecureRandomAlgorithm(String secureRandomAlgorithm) {
        this.secureRandomAlgorithm = secureRandomAlgorithm;
    }

    public String getSecureRandomProvider() {
        return secureRandomProvider;
    }

    public void setSecureRandomProvider(String secureRandomProvider) {
        this.secureRandomProvider = secureRandomProvider;
    }

    @Override
    protected void stopInternal() throws LifecycleException {
        log.trace("EXEC stopInternal();");

        setState(LifecycleState.STOPPING);

        this.sessionIdGenerator = null;

        if (!disableListeners) {
            eventListenerThread.stop();
        }

        pool.destroy();
    }

    protected String generateSessionId() {
        String result;

        try {
            do {
                result = sessionIdGenerator.generateSessionId();
            } while (findSession(result) != null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    @Override
    public void propertyChange(PropertyChangeEvent event) {
        log.trace(String.format("EXEC propertyChange(%s);", event));

        if (!(event.getSource() instanceof Context)) {
            return;
        }

        if (event.getPropertyName().equals("sessionTimeout")) {
            try {
                setMaxInactiveInterval((Integer) event.getNewValue() * 60);
            } catch (NumberFormatException e) {
                log.error(sm.getString("managerBase.sessionTimeout", event.getNewValue()));
            }
        }
    }
}
