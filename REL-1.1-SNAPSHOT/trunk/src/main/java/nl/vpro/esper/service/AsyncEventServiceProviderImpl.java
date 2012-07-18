/**
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.log4j.spi.LoggerFactory;

import com.espertech.esper.client.EPStatement;

public class AsyncEventServiceProviderImpl extends EventServiceProviderImpl implements AsyncEventServiceProvider {

    private final BlockingQueue queue;

    private ExecutorService executor;

    public AsyncEventServiceProviderImpl() {
        super();
        queue = new ArrayBlockingQueue(200);
    }

    public AsyncEventServiceProviderImpl(int queueSize) {
        super();
        queue = new ArrayBlockingQueue(queueSize);
    }

    public AsyncEventServiceProviderImpl(String name) {
        super(name);
        queue = new ArrayBlockingQueue(200);
    }

    public AsyncEventServiceProviderImpl(String name, int queueSize) {
        super(name);
        queue = new ArrayBlockingQueue(queueSize);
    }

    public AsyncEventServiceProviderImpl(String name, String eventPackage) {
        super(name, eventPackage);
        queue = new ArrayBlockingQueue(200);
    }

    public AsyncEventServiceProviderImpl(String name, String eventPackage, int queueSize) {
        super(name, eventPackage);
        queue = new ArrayBlockingQueue(queueSize);
    }

    @PostConstruct
    private void init() {
        executor = Executors.newSingleThreadExecutor();
        executor.submit(new EventHandler());
    }

    @PreDestroy
    private void shutDown() {
        executor.shutdown();
        epServiceProvider.destroy();
        queue.clear();
    }

    public void send(Object event) {
        try {
            queue.put(event);
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean offer(Object event) {
        return queue.offer(event);
    }

    private class EventHandler implements Runnable {
        public void run() {
            while(true) {
                try {
                    Object event = queue.take();
                    epRuntime.sendEvent(event);
                } catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
