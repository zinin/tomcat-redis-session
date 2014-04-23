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
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.session.Constants;
import org.apache.jasper.util.ExceptionUtils;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.res.StringManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import ru.zinin.redis.session.event.*;
import ru.zinin.redis.util.Base64Util;
import ru.zinin.redis.util.RedisSerializationUtil;

import javax.servlet.http.HttpSessionAttributeListener;
import javax.servlet.http.HttpSessionBindingEvent;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Date: 01.11.11 20:43
 *
 * @author Alexander V. Zinin (mail@zinin.ru)
 */
public class RedisEventListenerThread implements Runnable {
    private final Log log = LogFactory.getLog(RedisEventListenerThread.class);

    private static final StringManager sm = StringManager.getManager(Constants.Package);

    private RedisManager manager;

    private AtomicBoolean stopFlag = new AtomicBoolean(false);

    private JedisPubSub pubSub = new JedisPubSub() {
        @Override
        public void onMessage(String channel, String message) {
            byte[] bytes = Base64Util.decode(message);

            RedisSessionEvent event = RedisSerializationUtil.decode(bytes, manager.getContainerClassLoader());

            log.debug("Event from " + channel + ": " + event);

            if (event instanceof RedisSessionCreatedEvent) {
                sessionCreatedEvent((RedisSessionCreatedEvent) event);
            } else if (event instanceof RedisSessionDestroyedEvent) {
                sessionDestroyedEvent((RedisSessionDestroyedEvent) event);
            } else if (event instanceof RedisSessionAddAttributeEvent) {
                sessionAttributeAddEvent((RedisSessionAddAttributeEvent) event);
            } else if (event instanceof RedisSessionRemoveAttributeEvent) {
                sessionAttributeRemoveEvent((RedisSessionRemoveAttributeEvent) event);
            } else if (event instanceof RedisSessionReplaceAttributeEvent) {
                sessionAttributeReplaceEvent((RedisSessionReplaceAttributeEvent) event);
            }
        }

        @Override
        public void onPMessage(String pattern, String channel, String message) {
            log.debug("PMessage " + pattern + " from " + channel + ": " + message);
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            log.debug("Subscribe: " + channel + "; count: " + subscribedChannels);
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            log.debug("Unsubscribe: " + channel + "; count: " + subscribedChannels);
        }

        @Override
        public void onPUnsubscribe(String pattern, int subscribedChannels) {
            log.debug("PUnsubscribe: " + pattern + "; count: " + subscribedChannels);
        }

        @Override
        public void onPSubscribe(String pattern, int subscribedChannels) {
            log.debug("PSubscribe: " + pattern + "; count: " + subscribedChannels);
        }
    };

    public RedisEventListenerThread(RedisManager manager) {
        this.manager = manager;
    }

    @Override
    public void run() {
        while (!stopFlag.get()) {
            Jedis jedis = manager.getPool().getResource();
            try {
                log.debug("Subscribed to " + RedisSessionKeys.getSessionChannel());
                jedis.subscribe(pubSub, RedisSessionKeys.getSessionChannel());
                log.debug("Done.");

                manager.getPool().returnResource(jedis);
            } catch (Throwable e) {
                manager.getPool().returnBrokenResource(jedis);
                log.error(e.getMessage(), e);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    log.error(e.getMessage(), e);
                }
            }
        }

        log.debug("Exiting from listener thread...");
    }

    public void stop() {
        stopFlag.set(true);
        pubSub.unsubscribe();
    }

    private void fireContainerEvent(Context context, String type, Object data) throws Exception {
        if (context instanceof StandardContext) {
            context.fireContainerEvent(type, data);
        }
    }

    private void sessionCreatedEvent(RedisSessionCreatedEvent sessionCreatedEvent) {
        Context context = (Context) manager.getContainer();
        Object listeners[] = context.getApplicationLifecycleListeners();

        if (listeners != null) {
            RedisHttpSession session = new RedisHttpSession(sessionCreatedEvent.getId(), manager);

            HttpSessionEvent event = new HttpSessionEvent(session);
            //noinspection ForLoopReplaceableByForEach
            for (int i = 0; i < listeners.length; i++) {
                if (!(listeners[i] instanceof HttpSessionListener)) {
                    continue;
                }
                HttpSessionListener listener = (HttpSessionListener) listeners[i];
                try {
                    fireContainerEvent(context, "beforeSessionCreated", listener);

                    listener.sessionCreated(event);

                    fireContainerEvent(context, "afterSessionCreated", listener);
                } catch (Throwable t) {
                    ExceptionUtils.handleThrowable(t);
                    try {
                        fireContainerEvent(context, "afterSessionCreated", listener);
                    } catch (Exception e) {
                        // Ignore
                    }
                    manager.getContainer().getLogger().error(sm.getString("standardSession.sessionEvent"), t);
                }
            }
        }
    }

    private void sessionDestroyedEvent(RedisSessionDestroyedEvent sessionDestroyedEvent) {
        Context context = (Context) manager.getContainer();
        Object listeners[] = context.getApplicationLifecycleListeners();

        if (listeners != null) {
            RedisHttpSession session = new RedisHttpSession(sessionDestroyedEvent.getId(), manager);

            HttpSessionEvent event = new HttpSessionEvent(session);
            for (int i = 0; i < listeners.length; i++) {
                int j = (listeners.length - 1) - i;

                if (!(listeners[j] instanceof HttpSessionListener)) {
                    continue;
                }

                HttpSessionListener listener = (HttpSessionListener) listeners[j];
                try {
                    fireContainerEvent(context, "beforeSessionDestroyed", listener);

                    listener.sessionDestroyed(event);

                    fireContainerEvent(context, "afterSessionDestroyed", listener);
                } catch (Throwable t) {
                    ExceptionUtils.handleThrowable(t);
                    try {
                        fireContainerEvent(context, "afterSessionDestroyed", listener);
                    } catch (Exception e) {
                        // Ignore
                    }
                    manager.getContainer().getLogger().error(sm.getString("standardSession.sessionEvent"), t);
                }
            }
        }
    }

    private void sessionAttributeAddEvent(RedisSessionAddAttributeEvent sessionAddAttributeEvent) {
        Context context = (Context) manager.getContainer();
        Object listeners[] = context.getApplicationEventListeners();
        if (listeners != null) {
            RedisHttpSession session = new RedisHttpSession(sessionAddAttributeEvent.getId(), manager);

            //noinspection ForLoopReplaceableByForEach
            for (int i = 0; i < listeners.length; i++) {
                if (!(listeners[i] instanceof HttpSessionAttributeListener)) {
                    continue;
                }

                HttpSessionAttributeListener listener = (HttpSessionAttributeListener) listeners[i];
                try {
                    fireContainerEvent(context, "beforeSessionAttributeAdded", listener);
                    HttpSessionBindingEvent event = new HttpSessionBindingEvent(session, sessionAddAttributeEvent.getName(), sessionAddAttributeEvent.getValue());
                    listener.attributeAdded(event);
                    fireContainerEvent(context, "afterSessionAttributeAdded", listener);
                } catch (Throwable t) {
                    ExceptionUtils.handleThrowable(t);
                    try {
                        fireContainerEvent(context, "afterSessionAttributeAdded", listener);
                    } catch (Exception e) {
                        // Ignore
                    }
                    manager.getContainer().getLogger().error(sm.getString("standardSession.attributeEvent"), t);
                }
            }
        }
    }

    private void sessionAttributeRemoveEvent(RedisSessionRemoveAttributeEvent sessionRemoveAttributeEvent) {
        Context context = (Context) manager.getContainer();
        Object listeners[] = context.getApplicationEventListeners();
        if (listeners != null) {
            RedisHttpSession session = new RedisHttpSession(sessionRemoveAttributeEvent.getId(), manager);

            //noinspection ForLoopReplaceableByForEach
            for (int i = 0; i < listeners.length; i++) {
                if (!(listeners[i] instanceof HttpSessionAttributeListener)) {
                    continue;
                }

                HttpSessionAttributeListener listener = (HttpSessionAttributeListener) listeners[i];
                try {
                    fireContainerEvent(context, "beforeSessionAttributeRemoved", listener);
                    HttpSessionBindingEvent event = new HttpSessionBindingEvent(session, sessionRemoveAttributeEvent.getName(), sessionRemoveAttributeEvent.getValue());
                    listener.attributeRemoved(event);
                    fireContainerEvent(context, "afterSessionAttributeRemoved", listener);
                } catch (Throwable t) {
                    ExceptionUtils.handleThrowable(t);
                    try {
                        fireContainerEvent(context, "afterSessionAttributeRemoved", listener);
                    } catch (Exception e) {
                        // Ignore
                    }
                    manager.getContainer().getLogger().error(sm.getString("standardSession.attributeEvent"), t);
                }
            }
        }
    }

    private void sessionAttributeReplaceEvent(RedisSessionReplaceAttributeEvent sessionReplaceAttributeEvent) {
        Context context = (Context) manager.getContainer();
        Object listeners[] = context.getApplicationEventListeners();
        if (listeners != null) {
            RedisHttpSession session = new RedisHttpSession(sessionReplaceAttributeEvent.getId(), manager);

            //noinspection ForLoopReplaceableByForEach
            for (int i = 0; i < listeners.length; i++) {
                if (!(listeners[i] instanceof HttpSessionAttributeListener)) {
                    continue;
                }

                HttpSessionAttributeListener listener = (HttpSessionAttributeListener) listeners[i];
                try {
                    fireContainerEvent(context, "beforeSessionAttributeReplaced", listener);
                    HttpSessionBindingEvent event = new HttpSessionBindingEvent(session, sessionReplaceAttributeEvent.getName(), sessionReplaceAttributeEvent.getValue());
                    listener.attributeReplaced(event);
                    fireContainerEvent(context, "afterSessionAttributeReplaced", listener);
                } catch (Throwable t) {
                    ExceptionUtils.handleThrowable(t);
                    try {
                        fireContainerEvent(context, "afterSessionAttributeReplaced", listener);
                    } catch (Exception e) {
                        // Ignore
                    }
                    manager.getContainer().getLogger().error(sm.getString("standardSession.attributeEvent"), t);
                }
            }
        }
    }
}
