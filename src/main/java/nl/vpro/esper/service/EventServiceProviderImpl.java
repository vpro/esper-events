/*
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.service;

import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.*;
import com.espertech.esper.runtime.client.*;
import com.google.common.reflect.ClassPath;

import nl.vpro.esper.EsperEvent;

@Slf4j
public class EventServiceProviderImpl implements EventServiceProvider {


    protected final EPRuntime epRuntime;

    @Setter
    protected Set<Statement> statements = new LinkedHashSet<>();

    Configuration config = new Configuration();


    @SneakyThrows
    @lombok.Builder
    public EventServiceProviderImpl(
        String name,
        Set<String> eventPackages,
        Set<Class<? extends Annotation>> eventAnnotations)  {
        final Set<String> finalEventPackages;
        if (eventPackages == null) {
            finalEventPackages = Set.of("nl");
        } else {
            finalEventPackages = eventPackages;
        }
        ClassPath.from(getClass().getClassLoader())
            .getAllClasses()
            .stream()
            .filter(c -> ! c.getResourceName().equals("module-info.class"))
            .filter(c ->
                finalEventPackages.stream()
                    .anyMatch(p ->
                        c.getPackageName().equals(p) || c.getPackageName().startsWith(p + ".")
                    )
            )
            .map(ci -> {
                try {
                    return ci.load();
                } catch (Exception e) {
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .filter(c ->
                eventAnnotations == null ||
                    Arrays.stream(c.getAnnotations())
                        .anyMatch(a -> eventAnnotations.stream().anyMatch(ac -> ac.isInstance(a)))
            )
            .forEach((found) -> {
                log.info("Found event type {}", found);
                config.getCommon().addEventType(found);
            });
        epRuntime =  EPRuntimeProvider.getDefaultRuntime(config);
        init();
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

    public static class Builder {
        public Builder packages(String... eventPackages) {
            return eventPackages(Set.of(eventPackages));
        }

        public Builder packages(Package... eventPackages) {
            return eventPackages(Arrays.stream(eventPackages).map(Package::getName).collect(Collectors.toSet()));
        }

        public Builder esperEventAnnotation() {
            return eventAnnotations(Set.of(EsperEvent.class));
        }

    }

}
