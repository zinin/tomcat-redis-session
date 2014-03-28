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

package ru.zinin.redis.factory;

import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import java.util.Hashtable;

/**
 * <p>ObjectFactory for creation JedisPool objects.</p>
 * <p>Support properties:</p>
 * <ul>
 * <li>host - redis server hostname, "localhost" by default</li>
 * <li>port - redis server port, {@link redis.clients.jedis.Protocol#DEFAULT_PORT} by default</li>
 * <li>timeout - connection timeout in ms, {@link redis.clients.jedis.Protocol#DEFAULT_TIMEOUT} by default</li>
 * <li>database - connection timeout in ms, {@link redis.clients.jedis.Protocol#DEFAULT_DATABASE} by default</li>
 * <li>password - redis server password</li>
 * </ul>
 * <p/>
 * <p>Date: 30.10.11 22:15</p>
 *
 * @author Alexander V. Zinin (mail@zinin.ru)
 */
public class JedisPoolFactory implements ObjectFactory {
    private final Log log = LogFactory.getLog(JedisPoolFactory.class);

    /** {@inheritDoc} */
    @Override
    public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable<?, ?> environment) throws Exception {
        if ((obj == null) || !(obj instanceof Reference)) {
            return (null);
        }
        Reference ref = (Reference) obj;
        if (!"redis.clients.jedis.JedisPool".equals(ref.getClassName())) {
            return (null);
        }

        String host = "localhost";
        int port = Protocol.DEFAULT_PORT;
        int timeout = Protocol.DEFAULT_TIMEOUT;
        int database = Protocol.DEFAULT_DATABASE;
        String password = null;

        RefAddr hostRefAddr = ref.get("host");
        if (hostRefAddr != null) {
            host = hostRefAddr.getContent().toString();
        }
        RefAddr portRefAddr = ref.get("port");
        if (portRefAddr != null) {
            port = Integer.parseInt(portRefAddr.getContent().toString());
        }
        RefAddr timeoutRefAddr = ref.get("timeout");
        if (timeoutRefAddr != null) {
            timeout = Integer.parseInt(timeoutRefAddr.getContent().toString());
        }
        RefAddr databaseRefAddr = ref.get("database");
        if (databaseRefAddr != null) {
            database = Integer.parseInt(databaseRefAddr.getContent().toString());
        }
        RefAddr passwordRefAddr = ref.get("password");
        if (passwordRefAddr != null) {
            password = passwordRefAddr.getContent().toString();
        }

        log.debug("Creating pool...");
        log.debug("Host: " + host);
        log.debug("Port: " + port);
        log.debug("Timeout: " + timeout);
        log.trace("Password: " + password);

        JedisPoolConfig config = new JedisPoolConfig();
        return new JedisPool(config, host, port, timeout, password, database);
    }
}
