package ru.zinin.redis.session;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.servlet.ServletContext;
import java.util.HashSet;
import java.util.Set;

/**
 * Date: 12.11.11 12:08
 *
 * @author Alexander V. Zinin (mail@zinin.ru)
 */
public class RedisSessionTemplate {
    private JedisPool jedisPool;
    private ServletContext servletContext;
    private int dbIndex = 0;

    public RedisSessionTemplate() {
    }

    public RedisSessionTemplate(JedisPool jedisPool, ServletContext servletContext) {
        this.jedisPool = jedisPool;
        this.servletContext = servletContext;
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void setJedisPool(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public ServletContext getServletContext() {
        return servletContext;
    }

    public void setServletContext(ServletContext servletContext) {
        this.servletContext = servletContext;
    }

    public int getDbIndex() {
        return dbIndex;
    }

    public void setDbIndex(int dbIndex) {
        this.dbIndex = dbIndex;
    }

    private void verifyInitialization() {
        if (jedisPool == null) {
            throw new IllegalStateException("JedisPool is not initialized");
        }
        if (servletContext == null) {
            throw new IllegalStateException("ServletContext is not initialized");
        }
    }

    public Set<RedisHttpSession> getAllSessions() {
        verifyInitialization();

        Set<String> sessionIds;

        Jedis jedis = jedisPool.getResource();
        try {
            sessionIds = jedis.zrangeByScore(RedisSessionKeys.getSessionsKey(), 0, Double.MAX_VALUE);

            jedisPool.returnResource(jedis);
        } catch (Throwable e) {
            jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        }

        Set<RedisHttpSession> result = new HashSet<RedisHttpSession>(sessionIds.size());
        for (String sessionId : sessionIds) {
            RedisHttpSession session = new RedisHttpSession(sessionId, jedisPool, servletContext, dbIndex);
            if (session.isValid()) {
                result.add(session);
            }
        }

        return result;
    }

    public RedisHttpSession getSession(String id) {
        verifyInitialization();

        return new RedisHttpSession(id, jedisPool, servletContext, dbIndex);
    }
}
