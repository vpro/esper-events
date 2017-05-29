/*
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

import java.util.LinkedHashSet;
import java.util.Set;

import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.UpdateListener;

public class Statement {
    private final String epl;

    private EPStatement epStatement;

    private final Set<UpdateListener> listeners = new LinkedHashSet<>();

    public Statement(String epl) {
        this.epl = epl;
    }

    public String getEPL() {
        return epl;
    }

    public void setListeners(UpdateListener... listeners) {
        for(UpdateListener listener : listeners) {
            addListener(listener);
        }
    }

    public void addListener(UpdateListener listener) {
        listeners.add(listener);
        if(epStatement != null) {
            epStatement.addListener(listener);
        }
    }

    void setEPStatement(EPStatement epStatement) {
        this.epStatement = epStatement;
        for(UpdateListener listener : listeners) {
            epStatement.addListener(listener);
        }
    }
}
