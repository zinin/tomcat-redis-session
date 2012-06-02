# Tomcat Redis Session

## Introduction
Tomcat Redis Session is an implementation of
[Tomcat Manager Component](http://tomcat.apache.org/tomcat-7.0-doc/config/manager.html)
using [Redis](http://redis.io/) key-value store.

## Standalone configuration

First you must download the following dependencies:

- [Commons Pool](http://commons.apache.org/pool/)
- [Jedis](https://github.com/xetorthio/jedis)

Downloaded jar files put in CATALINA.HOME/lib folder or your web application lib folder (WEB-INF/lib).

Configure global context (CATALINA.HOME/conf/context.xml) or you web application context (META-INF/context.xml) for using Tomcat Redis Session Manager by inserting this line:

```xml
    <Manager className="ru.zinin.redis.session.RedisManager"/>
```

By default RedisManager looking for "pool/jedis" by JNDI. You can override this by adding property "jedisJndiName".

```xml
    <Manager className="ru.zinin.redis.session.RedisManager" jedisJndiName="custom/jndi/path"/>
```

## Embedded configuration

See our [Embedded example](https://github.com/zinin/tomcat-redis-session-example)

Add tomcat-redis-session as maven dependency:

```xml
    <dependency>
        <groupId>ru.zinin</groupId>
        <artifactId>tomcat-redis-session</artifactId>
        <version>0.4</version>
    </dependency>
```

Use it:
    
```java
    RedisManager redisManager = new RedisManager();
    redisManager.setDisableListeners(true);
    redisManager.setDbIndex(1);
    ctx.setManager(redisManager);
```

## Contacts

If you have questions you can [mail me](mailto:mail@zinin.ru)
[Bug tracker](https://github.com/zinin/tomcat-redis-session/issues?state=open)

## License
Copyright 2011 Alexander V. Zinin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
