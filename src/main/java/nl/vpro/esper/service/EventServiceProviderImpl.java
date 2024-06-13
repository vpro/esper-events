/*
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

import lombok.Setter;
import lombok.SneakyThrows;

import java.util.*;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.*;
import com.espertech.esper.runtime.client.*;
import com.google.common.reflect.ClassPath;


public class EventServiceProviderImpl implements EventServiceProvider {


    protected final EPRuntime epRuntime;

    @Setter
    protected Set<Statement> statements = new LinkedHashSet<>();

    Configuration config = new Configuration();


    public EventServiceProviderImpl() {
        this(null);
    }

    public EventServiceProviderImpl(String name)  {
        this(name, "nl.vpro.esper.event");
    }

    @SneakyThrows
    public EventServiceProviderImpl(String name, String... eventPackages)  {
        Set<String> eventPackagesSet = Set.of(eventPackages);
        ClassPath.from(ClassLoader.getSystemClassLoader())
            .getAllClasses()
            .stream()
            .filter(c -> eventPackagesSet.contains(c.getPackageName()))
            .map(ClassPath.ClassInfo::load)
            .forEach(config.getCommon()::addEventType);
        epRuntime =  EPRuntimeProvider.getDefaultRuntime(config);
        init();
    }

     public EventServiceProviderImpl(String name, Package... eventPackages) {
         this(name, Arrays.stream(eventPackages).map(Package::getName).toArray(String[]::new));
    }

    @SneakyThrows
    @PostConstruct
    private void init() {
        for(Statement statement : statements) {
            initStatement(statement);
        }
    }

    @PreDestroy
    private void shutDown() {
        epRuntime.destroy();
    }

    @Override
    public void send(Object event) {
        epRuntime.getEventService().sendEventBean(event, event.getClass().getSimpleName());
    }

    @SneakyThrows
    @Override
    public void addStatement(Statement statement) {
        this.statements.add(statement);
        initStatement(statement);
    }

    private void initStatement(Statement statement) throws EPCompileException, EPDeployException {
        CompilerArguments args = new CompilerArguments();
        args.getPath().add(epRuntime.getRuntimePath());
        args.setConfiguration(config);
        EPCompiler compiler = EPCompilerProvider.getCompiler();

        EPDeployment deployment = epRuntime.getDeploymentService().deploy(
            compiler.compile(statement.getEPL(), args)
        );
        statement.setEPStatement(deployment.getStatements()[0]);
    }

}
