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

package ru.zinin.redis.session.event;

import java.io.Serializable;

/**
 * Date: 01.11.11 22:14
 *
 * @author Alexander V. Zinin (mail@zinin.ru)
 */
public abstract class RedisSessionAttributeEvent extends RedisSessionEvent {
    private String name;
    private Serializable value;

    protected RedisSessionAttributeEvent(String id, String name, Serializable value) {
        super(id);
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public Serializable getValue() {
        return value;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getCanonicalName());
        sb.append("{id='").append(getId()).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", value='").append(value).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
