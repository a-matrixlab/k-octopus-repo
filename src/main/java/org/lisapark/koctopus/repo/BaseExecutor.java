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

import com.google.gson.Gson;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lisapark.koctopus.core.ProcessingException;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.graph.Graph;
import org.lisapark.koctopus.core.graph.api.Vocabulary;
import org.lisapark.koctopus.core.processor.AbstractProcessor;
import org.lisapark.koctopus.core.transport.redis.RedisTransport;
import org.lisapark.koctopus.core.sink.external.ExternalSink;
import org.lisapark.koctopus.core.source.external.AbstractExternalSource;

/**
 *
 * @author alexmy
 */
public class BaseExecutor {
    
    static final Logger LOG = Logger.getLogger(BaseExecutor.class.getName());

    enum Status {
        SUCCESS(200),
        ERROR(400);
        private final int statusCode;

        Status(int statusCode) {
            this.statusCode = statusCode;
        }

        public int getStatusCode() {
            return this.statusCode;
        }
    }
    
    private final RepoCache koCache;
    
    public BaseExecutor(RepoCache koCache){
        this.koCache = koCache;
    }

    /**
     *
     * @param json
     * @return
     * @throws ValidationException
     * @throws ProcessingException
     * @throws java.lang.InterruptedException
     */
    public String process(String json) throws ValidationException, ProcessingException, InterruptedException {
        String result = null;
        Gnode gnode = (Gnode) new Gnode().fromJson(json);
        String trnsUrl = gnode.getTransportUrl();
        RedisTransport runtime = new RedisTransport(trnsUrl, System.out, System.err);

        try {
            String type;
            switch (gnode.getLabel()) {
                case Vocabulary.SOURCE:
                    type = gnode.getType();
                    AbstractExternalSource sourceIns = koCache.getSourceCache().get(type);
                    AbstractExternalSource source = (AbstractExternalSource) sourceIns.newInstance(gnode);
                    result = new Gson().toJson(gnode);
                    source.compile(source).startProcessingEvents(runtime);

                    break;
                case Vocabulary.PROCESSOR:
                    type = gnode.getType();
                    AbstractProcessor processorIns = koCache.getProcessorCache().get(type);
                    AbstractProcessor processor = (AbstractProcessor) processorIns.newInstance(gnode);
                    result = new Gson().toJson(gnode);
                    processor.compile(processor).processEvent(runtime);

                    break;
                case Vocabulary.SINK:
                    type = gnode.getType();
                    ExternalSink sinkIns = koCache.getSinkCache().get(type);
                    ExternalSink sink = (ExternalSink) sinkIns.newInstance(gnode);
                    result = new Gson().toJson(gnode);
                    sink.compile(sink).processEvent(runtime);

                    break;
                case Vocabulary.MODEL:
                    Graph graph = (Graph) new Graph().fromJson(json);
                    type = graph.getType();
                    AbstractRunner runner = (AbstractRunner) Class.forName(type).newInstance();
                    runner.setGraph(graph);
                    runner.setKoCache(koCache);
                    runner.init();
                    result = new Gson().toJson((Gnode)graph, Gnode.class);
                    runner.execute();

                    break;
                default:
                    break;
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | ExecutionException ex) {
            LOG.log(Level.SEVERE, ex.getMessage());
        }
        return result;
    }
}
