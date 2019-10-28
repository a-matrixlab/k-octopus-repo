/* 
 * Copyright (C) 2019 Lisa Park, Inc. (www.lisa-park.net)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.lisapark.koctopus.repo;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lisapark.koctopus.core.processor.AbstractProcessor;
import org.lisapark.koctopus.core.sink.external.AbstractExternalSink;
import org.lisapark.koctopus.core.source.external.AbstractExternalSource;

/**
 *
 * @author alexmy
 */
public class RedisRepository extends AbstractOctopusRepository {

    private static final Logger LOG = Logger.getLogger(RedisRepository.class.getName());

    static final String SOURCE_TOP_PACKAGE = "org.lisapark.koctopus.processors.source";
    static final String SINK_TOP_PACKAGE = "org.lisapark.koctopus.processors.sink";
    static final String PROCESSOR_TOP_PACKAGE = "org.lisapark.koctopus.processors.processor";
    static final String PIPE_TOP_PACKAGE = "org.lisapark.koctopus.processors.pipe";

    private static final String[] REPO_PATH = {"file:///home/alexmy/.m2/repository/k-octopus/k-octopus-processors/0.7.3/k-octopus-processors-0.7.3-jar-with-dependencies.jar"};

    public RedisRepository() {
        super();
    }

    @Override
    public synchronized void loadAllProcessors(List<AbstractExternalSource> sources, List<AbstractExternalSink> sinks, List<AbstractProcessor> processors) {
        List<String> repoPathList = new ArrayList<>(Arrays.asList(REPO_PATH));

        repoPathList.forEach((String item) -> {
            try {
                URLClassLoader child = new URLClassLoader(new URL[]{new URL(item)}, this.getClass().getClassLoader());
                ClassPath classpath = ClassPath.from(child);
                ImmutableSet<ClassInfo> allClasses = classpath.getAllClasses();
                for (ClassInfo classInfo : allClasses) {
                    if (classInfo.getName().startsWith(SOURCE_TOP_PACKAGE)) {
                        try {
                            if (classInfo.getName().contains("$")) {

                            } else {
                                classInfo.load();
                                AbstractExternalSource source = (AbstractExternalSource) Class.forName(classInfo.getName(), true, child).newInstance();
                                sources.add(source);
                                LOG.log(Level.INFO, "Sources: {0}", classInfo);
                            }
                        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
                            LOG.log(Level.SEVERE, ex.getMessage());
                        }
                    } else if (classInfo.getName().startsWith(SINK_TOP_PACKAGE)) {
                        try {
                            if (classInfo.getName().contains("$")) {

                            } else {
                                classInfo.load();
                                AbstractExternalSink sink = (AbstractExternalSink) Class.forName(classInfo.getName(), true, child).newInstance();
                                sinks.add(sink);
                                LOG.log(Level.INFO, "Sinks: {0}", classInfo);
                            }
                        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
                            LOG.log(Level.SEVERE, ex.getMessage());
                        }
                    } else if (classInfo.getName().startsWith(PROCESSOR_TOP_PACKAGE) || classInfo.getName().startsWith(PIPE_TOP_PACKAGE)) {

                        if (classInfo.getName().contains("$")) {

                        } else {
                            try {
                                classInfo.load();
                                AbstractProcessor processor = (AbstractProcessor) (Class.forName(classInfo.getName(), true, child)).newInstance();
                                processors.add(processor);
                                LOG.log(Level.INFO, "Processors: {0}", classInfo);
                            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
                                LOG.log(Level.SEVERE, ex.getMessage());
                            }
                        }
                    } else {
                    }
                }
            } catch (MalformedURLException ex) {
                LOG.log(Level.SEVERE, ex.getMessage());
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, ex.getMessage());
            }
        });
    }
}
