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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.net.URLClassLoader;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.graph.Graph;
import org.lisapark.koctopus.core.graph.api.Vocabulary;
import org.lisapark.koctopus.core.processor.AbstractProcessor;
import org.lisapark.koctopus.core.sink.external.AbstractExternalSink;
import org.lisapark.koctopus.core.source.external.AbstractExternalSource;
import org.lisapark.koctopus.repo.utils.RepoUtils;

/**
 * There are three types of processors, so we need 3 caches for them. Plus we
 * need to keep references to all JARs loaded to the system. Each jar will be
 * identified with URL string of this jar. JAR's URLs are part of configuration.
 * On K8s they should be managed with ConfigMap kube-api.
 *
 * @author alexmy
 */
public class RepoCache {

    private LoadingCache<String, AbstractProcessor> processorCache;
    private LoadingCache<String, AbstractExternalSource> sourceCache;
    private LoadingCache<String, AbstractExternalSink> sinkCache;

    private final RedisRepository repo = new RedisRepository();

    ;

    public RepoCache() {
        initCache();
    }

    /**
     *
     */
    private void initCache() {
        processorCache = CacheBuilder.newBuilder()
                //                .maximumSize(100) // maximum 100 records can be cached
                //                .expireAfterAccess(30, TimeUnit.MINUTES) // cache will expire after 30 minutes of access
                .build(new CacheLoader<String, AbstractProcessor>() {  // build the cacheloader

                    @Override
                    public AbstractProcessor load(String className) throws Exception {
                        //make the expensive call
                        return repo.getAbstractProcessorByName(RepoUtils.getPair(className));
                    }

                });

        sourceCache = CacheBuilder.newBuilder()
                //                .maximumSize(100) // maximum 100 records can be cached
                //                .expireAfterAccess(30, TimeUnit.MINUTES) // cache will expire after 30 minutes of access
                .build(new CacheLoader<String, AbstractExternalSource>() {  // build the cacheloader

                    @Override
                    public AbstractExternalSource load(String className) throws Exception {
                        //make the expensive call
                        return repo.getAbstractExternalSourceByName(RepoUtils.getPair(className));
                    }
                });

        sinkCache = CacheBuilder.newBuilder()
                //                .maximumSize(100) // maximum 100 records can be cached
                //                .expireAfterAccess(30, TimeUnit.MINUTES) // cache will expire after 30 minutes of access
                .build(new CacheLoader<String, AbstractExternalSink>() {  // build the cacheloader

                    @Override
                    public AbstractExternalSink load(String className) throws Exception {
                        //make the expensive call
                        return repo.getAbstractExternalSinkByName(RepoUtils.getPair(className));
                    }
                });
    }

    /**
     * Pre-populates Cache with all processors from gnode that are not loaded
     * yet.
     *
     * @param gnode
     */
    public void loadModelProcessors(Gnode gnode) {
        // Load all processors from the gnode
        if (gnode.getLabel().equalsIgnoreCase(Vocabulary.MODEL)) {
            String graphJson = gnode.toJson().toString();
            Graph graph = new Graph().fromJson(graphJson);

            graph.getNodes().stream().forEachOrdered((Gnode _gnode) -> {
                loadProcessor(_gnode);
            });
        } else {
            loadProcessor(gnode);
        }
    }

    public void loadProcessor(Gnode gnode) {

    }

    /**
     * @return the processorCache
     */
    public LoadingCache<String, AbstractProcessor> getProcessorCache() {
        return processorCache;
    }

    /**
     * @return the sourceCache
     */
    public LoadingCache<String, AbstractExternalSource> getSourceCache() {
        return sourceCache;
    }

    /**
     * @return the sinkCache
     */
    public LoadingCache<String, AbstractExternalSink> getSinkCache() {
        return sinkCache;
    }
}