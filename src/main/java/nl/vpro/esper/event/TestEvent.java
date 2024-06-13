/*
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.event;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class TestEvent {
    private String name;

    public TestEvent(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("TestEvent");
        sb.append("{name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
