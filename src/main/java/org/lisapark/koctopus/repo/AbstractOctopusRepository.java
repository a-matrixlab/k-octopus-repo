/* 
 * Copyright (C) 2013 Lisa Park, Inc. (www.lisa-park.net)
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

import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;
import org.lisapark.koctopus.core.OctopusRepository;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lisapark.koctopus.core.processor.AbstractProcessor;
import org.lisapark.koctopus.core.sink.external.AbstractExternalSink;
import org.lisapark.koctopus.core.source.external.AbstractExternalSource;
import org.lisapark.koctopus.util.Pair;

public abstract class AbstractOctopusRepository implements OctopusRepository {

    static final String DEFAULT_REPO_PATH = "file:///home/alexmy/.m2/repository/k-octopus/k-octopus-processors/0.7.3/k-octopus-processors-0.7.3-jar-with-dependencies.jar";

    private static final Logger LOG = Logger.getLogger(AbstractOctopusRepository.class.getName());

    /**
     *
     * @param jar_type
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws MalformedURLException
     */
    @Override
    public synchronized AbstractExternalSource getAbstractExternalSourceByName(Pair<String, String> jar_type)
            throws InstantiationException, IllegalAccessException, MalformedURLException {

        AbstractExternalSource _source = null;
        try {
            _source = (AbstractExternalSource) Class.forName(jar_type.getSecond()).newInstance();
        } catch (ClassNotFoundException ex2) {
            String repoPath = getRepoPath(jar_type);
            URL url = new URL(repoPath);
            try (URLClassLoader loader = new URLClassLoader(new URL[]{url})) {
                ClassPath classpath = ClassPath.from(loader);
                for (ClassInfo classInfo : classpath.getAllClasses()) {
                    if (classInfo.getName().contains(jar_type.getSecond())) {
                        try {
                            Class<?> clazz = classInfo.load();
                            if (classInfo.toString().indexOf("$") <= 0) {
                                _source = (AbstractExternalSource) clazz.newInstance();
                            }
                        } catch (InstantiationException | IllegalAccessException ex) {
                            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                }
            } catch (IOException ex1) {
                LOG.log(Level.SEVERE, ex1.getMessage());
            }
        }

        if (_source != null) {
            return _source.newTemplate();
        }
        return _source;
    }

    /**
     *
     * @param jar_type
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws MalformedURLException
     */
    @Override
    public synchronized AbstractExternalSink getAbstractExternalSinkByName(Pair<String, String> jar_type) throws InstantiationException, IllegalAccessException, MalformedURLException {
        AbstractExternalSink sink = null;
        try {
            sink = (AbstractExternalSink) Class.forName(jar_type.getSecond()).newInstance();
        } catch (ClassNotFoundException ex) {
            String repoPath = getRepoPath(jar_type);
            URL url = new URL(repoPath);
            try (URLClassLoader loader = new URLClassLoader(new URL[]{url})) {
                ClassPath classpath = ClassPath.from(loader);
                for (ClassInfo classInfo : classpath.getAllClasses()) {
                    if (classInfo.getName().contains(jar_type.getSecond())) {
                        try {
                            Class<?> clazz = classInfo.load();
                            if (classInfo.toString().indexOf("$") <= 0) {
                                sink = (AbstractExternalSink) clazz.newInstance();
                            }
                        } catch (InstantiationException | IllegalAccessException ex2) {
                            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex2);
                        }
                    }
                }
            } catch (IOException ex1) {
                LOG.log(Level.SEVERE, ex1.getMessage());
            }
        }
        if (sink != null) {
            return sink.newTemplate();
        }
        return sink;
    }

    /**
     *
     * @param jar_type
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws MalformedURLException
     */
    @Override
    public synchronized AbstractProcessor getAbstractProcessorByName(Pair<String, String> jar_type) throws InstantiationException, IllegalAccessException, MalformedURLException {
        AbstractProcessor processor = null;
        try {
            processor = (AbstractProcessor) Class.forName(jar_type.getSecond()).newInstance();
        } catch (ClassNotFoundException ex) {
            String repoPath = getRepoPath(jar_type);
            URL url = new URL(repoPath);
            try (URLClassLoader loader = new URLClassLoader(new URL[]{url})) {
                ClassPath classpath = ClassPath.from(loader);
                for (ClassInfo classInfo : classpath.getAllClasses()) {
                    if (classInfo.getName().contains(jar_type.getSecond())) {
                        try {
                            Class<?> clazz = classInfo.load();
                            if (classInfo.toString().indexOf("$") <= 0) {
                                processor = (AbstractProcessor) clazz.newInstance();
                            }
                        } catch (InstantiationException | IllegalAccessException ex2) {
                            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex2);
                        }
                    }
                }
            } catch (IOException ex1) {
                LOG.log(Level.SEVERE, ex1.getMessage());
            }
        }

        if (processor != null) {
            return processor.newTemplate();
        }
        return processor;
    }

    /**
     *
     * @param jar_type
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws MalformedURLException
     */
    @Override
    public synchronized Object getObjectByName(Pair<String, String> jar_type) throws InstantiationException, IllegalAccessException, MalformedURLException {
        Object object = null;
        try {
            object = Class.forName(jar_type.getSecond()).newInstance();
        } catch (ClassNotFoundException ex) {
            String repoPath = getRepoPath(jar_type);
            URL url = new URL(repoPath);
            try (URLClassLoader loader = new URLClassLoader(new URL[]{url})) {
                ClassPath classpath = ClassPath.from(loader);
                for (ClassInfo classInfo : classpath.getAllClasses()) {
                    if (classInfo.getName().contains(jar_type.getSecond())) {
                        try {
                            Class<?> clazz = classInfo.load();
                            if (classInfo.toString().indexOf("$") <= 0) {
                                object = clazz.newInstance();
                            }
                        } catch (InstantiationException | IllegalAccessException ex2) {
                            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex2);
                        }
                    }
                }
            } catch (IOException ex1) {
                LOG.log(Level.SEVERE, ex1.getMessage());
            }
        }

        return object;
    }

    /**
     *
     * @param jar_type
     * @return
     */
    private String getRepoPath(Pair<String, String> jar_type) {
        String repoPath;
        if (jar_type.getFirst() == null) {
            repoPath = DEFAULT_REPO_PATH;
        } else {
            repoPath = jar_type.getFirst();
        }
        return repoPath;
    }
}
