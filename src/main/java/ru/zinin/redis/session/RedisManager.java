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
import org.apache.catalina.mbeans.MBeanUtils;
import org.apache.catalina.session.Constants;
import org.apache.catalina.util.LifecycleMBeanBase;
import org.apache.catalina.util.SessionIdGenerator;
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
public class RedisManager extends LifecycleMBeanBase implements Manager, PropertyChangeListener {
    private final Log log = LogFactory.getLog(RedisManager.class);

    private static final String info = "RedisManager/1.0";
    private static final StringManager sm = StringManager.getManager(Constants.Package);

    private Container container;

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
    public Container getContainer() {
        return container;
    }

    @Override
    public void setContainer(Container container) {
        log.trace(String.format("EXEC setContainer(%s);", container));

        // De-register from the old Container (if any)
        if ((this.container != null) && (this.container instanceof Context)) {
            this.container.removePropertyChangeListener(this);
        }

        Container oldContainer = this.container;
        this.container = container;
        support.firePropertyChange("container", oldContainer, this.container);

        // Register with the new Container (if any)
        if ((this.container != null) && (this.container instanceof Context)) {
            setMaxInactiveInterval(((Context) this.container).getSessionTimeout() * 60);
            this.container.addPropertyChangeListener(this);
        }
    }

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
    public String getInfo() {
        log.trace("EXEC getInfo();");

        return info;
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
    public int getSessionIdLength() {
        log.trace("EXEC getSessionIdLength();");

        return sessionIdLength;
    }

    @Override
    public void setSessionIdLength(int idLength) {
        log.trace(String.format("EXEC setSessionIdLength(%d);", idLength));

        int oldSessionIdLength = this.sessionIdLength;
        this.sessionIdLength = idLength;
        support.firePropertyChange("sessionIdLength", Integer.valueOf(oldSessionIdLength), Integer.valueOf(this.sessionIdLength));
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
    public void changeSessionId(Session session) {
        log.trace(String.format("EXEC changeSessionId(%s);", session));

        String oldId = session.getIdInternal();
        session.setId(generateSessionId(), false);
        String newId = session.getIdInternal();

        container.fireContainerEvent(Context.CHANGE_SESSION_ID_EVENT, new String[]{oldId, newId});
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

    @Override
    protected String getDomainInternal() {
        log.trace("EXEC getDomainInternal();");

        return MBeanUtils.getDomain(container);
    }

    @Override
    protected String getObjectNameKeyProperties() {
        log.trace("EXEC getObjectNameKeyProperties();");

        StringBuilder name = new StringBuilder("type=Manager");

        if (container instanceof Context) {
            name.append(",context=");
            String contextName = container.getName();
            if (!contextName.startsWith("/")) {
                name.append('/');
            }
            name.append(contextName);

            Context context = (Context) container;
            name.append(",host=");
            name.append(context.getParent().getName());
        } else {
            // Unlikely / impossible? Handle it to be safe
            name.append(",container=");
            name.append(container.getName());
        }

        return name.toString();
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

        sessionIdGenerator = new SessionIdGenerator();
        sessionIdGenerator.setJvmRoute(getJvmRoute());
        sessionIdGenerator.setSecureRandomAlgorithm(getSecureRandomAlgorithm());
        sessionIdGenerator.setSecureRandomClass(getSecureRandomClass());
        sessionIdGenerator.setSecureRandomProvider(getSecureRandomProvider());
        sessionIdGenerator.setSessionIdLength(getSessionIdLength());

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

    public Engine getEngine() {
        Engine e = null;
        for (Container c = getContainer(); e == null && c != null; c = c.getParent()) {
            if (c instanceof Engine) {
                e = (Engine) c;
            }
        }
        return e;
    }

    public String getJvmRoute() {
        Engine e = getEngine();
        return e == null ? null : e.getJvmRoute();
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

    protected ClassLoader getContainerClassLoader() {
        if (container instanceof Context) {
            Context containerContext = (Context)container;
            Loader contextLoader = containerContext.getLoader();
            if (contextLoader == null) {
                return null;
            }
            return contextLoader.getClassLoader();
        }
        return null;
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
