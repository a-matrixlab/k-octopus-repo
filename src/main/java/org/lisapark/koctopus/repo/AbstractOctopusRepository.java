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

import org.lisapark.koctopus.core.OctopusRepository;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lisapark.koctopus.core.processor.AbstractProcessor;
import org.lisapark.koctopus.core.sink.external.AbstractExternalSink;
import org.lisapark.koctopus.core.source.external.AbstractExternalSource;
import org.lisapark.koctopus.util.Pair;

public abstract class AbstractOctopusRepository implements OctopusRepository {

    private static final Logger LOG = Logger.getLogger(AbstractOctopusRepository.class.getName());
    
    @Override
    public List<AbstractExternalSink> getAllExternalSinkTemplates(List<String> sink_jars) {

        return Lists.newArrayList(new AbstractExternalSink[]{ //            ConsoleFromRedis.newTemplate(),
        //            LuceneBaseIndex.newTemplate()//,             DatabaseSink.newTemplate()
        });
    }

    @Override
    public List<AbstractExternalSource> getAllExternalSourceTemplates(List<String> source_jars) {

//        source_jars.forEach((String source) -> {
//            try {
//                URLClassLoader child = new URLClassLoader(new URL[]{new URL(source)}, this.getClass().getClassLoader());
//                ClassPath classpath = ClassPath.from(child);
//                classpath.getTopLevelClasses(SOURCE_TOP_PACKAGE).forEach((classInfo) -> {
//                    
//                });
//            } catch (MalformedURLException ex) {
//                LOG.log(Level.SEVERE, ex.getMessage());
//            } catch (IOException ex) {
//                LOG.log(Level.SEVERE, ex.getMessage());
//            }
//        });
//        ClassPath classpath = ClassPath.from(classloader);
//        classpath.getTopLevelClasses("com.mycomp.mypackage").forEach((classInfo) -> {
//            System.out.println(classInfo.getName());
//        });
        return Lists.newArrayList(new AbstractExternalSource[]{ //            DocDirSource.newTemplate(),
        //            KickStarterSource.newTemplate(),
        //            GdeltZipSource.newTemplate(),
        //            RedisQuittokenSource.newTemplate(),
        //            RTCSource.newTemplate(),
        //            SqlQuerySource.newTemplate(),
        //            TestSource.newTemplate(),
        //            TestSourceRedis.newTemplate(), //            TestRandomBinarySource.newTemplate()
        });
    }

    @Override
    public List<AbstractProcessor> getAllProcessorTemplates(List<String> processors_jars) {

        return Lists.newArrayList(new AbstractProcessor[]{ //            Crossing.newTemplate(),
        //            ForecastSRM.newTemplate(),
        //            LinearRegressionProcessor.newTemplate(),
        //            PearsonsCorrelationProcessor.newTemplate(),
        //            PipeDouble.newTemplate(),
        //            PipeString.newTemplate(),
        //            PipeStringDouble.newTemplate(),
        //            RTCcontroller.newTemplate(),
        //            SmaOld.newTemplate(),
        //                    SmaRedis.newTemplate()
        });
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
    public AbstractExternalSource getAbstractExternalSourceByName(Pair<String, String> jar_type)
            throws InstantiationException, IllegalAccessException, MalformedURLException {

        AbstractExternalSource source = null;
        try {
            source = (AbstractExternalSource) Class.forName(jar_type.getSecond()).newInstance();
        } catch (ClassNotFoundException ex) {
            URL url = new URL(jar_type.getFirst());
            try (URLClassLoader loader = new URLClassLoader(new URL[]{url})) {
                Class cl = Class.forName(jar_type.getSecond(), true, loader);
                source = (AbstractExternalSource) cl.newInstance();
            } catch (IOException | ClassNotFoundException ex1) {
                LOG.log(Level.SEVERE, ex1.getMessage());
            }
            LOG.log(Level.SEVERE, ex.getMessage());
        }

        return source;
    }

    @Override
    public AbstractExternalSink getAbstractExternalSinkByName(Pair<String, String> jar_type) throws InstantiationException, IllegalAccessException, MalformedURLException {
        AbstractExternalSink sink = null;
        try {
            sink = (AbstractExternalSink) Class.forName(jar_type.getSecond()).newInstance();
        } catch (ClassNotFoundException ex) {
            URL url = new URL(jar_type.getFirst());
            try (URLClassLoader loader = new URLClassLoader(new URL[]{url})) {
                Class cl = Class.forName(jar_type.getSecond(), true, loader);
                sink = (AbstractExternalSink) cl.newInstance();
            } catch (IOException | ClassNotFoundException ex1) {
                LOG.log(Level.SEVERE, ex1.getMessage());
            }
            LOG.log(Level.SEVERE, ex.getMessage());
        }

        return sink;
    }

    @Override
    public AbstractProcessor getAbstractProcessorByName(Pair<String, String> jar_type) throws InstantiationException, IllegalAccessException, MalformedURLException {
        AbstractProcessor processor = null;
        try {
            processor = (AbstractProcessor) Class.forName(jar_type.getSecond()).newInstance();
        } catch (ClassNotFoundException ex) {
            URL url = new URL(jar_type.getFirst());
            try (URLClassLoader loader = new URLClassLoader(new URL[]{url})) {
                Class cl = Class.forName(jar_type.getSecond(), true, loader);
                processor = (AbstractProcessor) cl.newInstance();
            } catch (IOException | ClassNotFoundException ex1) {
                LOG.log(Level.SEVERE, ex1.getMessage());
            }
            LOG.log(Level.SEVERE, ex.getMessage());
        }

        return processor;
    }

    @Override
    public Object getObjectByName(Pair<String, String> jar_type) throws InstantiationException, IllegalAccessException, MalformedURLException {
        Object object = null;
        try {
            object = Class.forName(jar_type.getSecond()).newInstance();
        } catch (ClassNotFoundException ex) {
            URL url = new URL(jar_type.getFirst());
            try (URLClassLoader loader = new URLClassLoader(new URL[]{url})) {
                Class cl = Class.forName(jar_type.getSecond(), true, loader);
                object = cl.newInstance();
            } catch (IOException | ClassNotFoundException ex1) {
                LOG.log(Level.SEVERE, ex1.getMessage());
            }
            LOG.log(Level.SEVERE, ex.getMessage());
        }

        return object;
    }
}
