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
package org.lisapark.koctopus.runner;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lisapark.koctopus.core.ProcessingException;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.graph.Graph;
import org.lisapark.koctopus.core.graph.api.Vocabulary;
import org.lisapark.koctopus.core.processor.AbstractProcessor;
import org.lisapark.koctopus.core.runtime.RuntimeUtils;
import org.lisapark.koctopus.core.transport.redis.RedisTransport;
import org.lisapark.koctopus.core.sink.external.AbstractExternalSink;
import org.lisapark.koctopus.core.source.external.AbstractExternalSource;

/**
 *
 * @author alexmy
 */
public class OctopusRunner extends AbstractRunner<Integer> {

    static final Logger LOG = Logger.getLogger(OctopusRunner.class.getName());
    
    
    public OctopusRunner() {
        super();
    }

    public OctopusRunner(Graph graph) {
        super(graph);
    }

    @Override
    public String processNode(Gnode gnode, boolean forward) {
        String trnsUrl = gnode.getTransportUrl();
        RedisTransport transport = new RedisTransport(trnsUrl, getStandardOut(), getStandardError());
        Integer status;
        try {
            String type;
            switch (gnode.getLabel()) {
                case Vocabulary.SOURCE:
                    type = gnode.getType();
                    AbstractExternalSource sourceIns = getKoCache().getSourceCache().get(type);
                    AbstractExternalSource source = (AbstractExternalSource) sourceIns.newInstance(gnode);

                    String sourceUrl = source.getServiceUrl();
                    if (sourceUrl == null || sourceUrl.trim().isEmpty()) {
                        status = (Integer) source.compile(source).startProcessingEvents(transport);
                    } else {
                        try {
                            RuntimeUtils.runRemote(sourceUrl, gnode.toJson().toString());
                            status = Vocabulary.COMPLETE;
                        } catch (IOException ex) {
                            status = processException(ex, sourceUrl);
                        }
                    }
                    getNodeStatus().put(gnode.getId(), status);
                    break;

                case Vocabulary.PROCESSOR:
                    type = gnode.getType();
                    AbstractProcessor processorIns = getKoCache().getProcessorCache().get(type);
                    AbstractProcessor processor = (AbstractProcessor) processorIns.newInstance(gnode);
                    
                    String procUrl = processor.getServiceUrl();
                    if (procUrl == null || procUrl.trim().isEmpty()) {
                        status = (Integer) processor.compile(processor).processEvent(transport);
                    } else {
                        try {
                            RuntimeUtils.runRemote(procUrl, gnode.toJson().toString());
                            status = Vocabulary.COMPLETE;
                        } catch (IOException ex) {
                            status = processException(ex, procUrl);
                        }
                    }
                    getNodeStatus().put(gnode.getId(), status);

                    break;
                case Vocabulary.SINK:
                    type = gnode.getType();
                    AbstractExternalSink sinkIns = getKoCache().getSinkCache().get(type);
                    AbstractExternalSink sink = (AbstractExternalSink) sinkIns.newInstance(gnode);
                    
                    String sinkUrl = sink.getServiceUrl();
                    if (sinkUrl == null || sinkUrl.trim().isEmpty()) {
                    status = (Integer) sink.compile(sink).processEvent(transport);
                    } else {
                        try {
                            RuntimeUtils.runRemote(sinkUrl, gnode.toJson().toString());
                            status = Vocabulary.COMPLETE;
                        } catch (IOException ex) {
                            status = processException(ex, sinkUrl);
                        }
                    }
                    getNodeStatus().put(gnode.getId(), status);

                    break;
                default:
                    break;
            }
        } catch (ValidationException | ProcessingException | ExecutionException ex) {
            LOG.log(Level.SEVERE, ex.getMessage());
        }

        return new Gson().toJson(gnode);
    }

    private Integer processException(Exception ex, String procUrl) {
        Integer status;
        List<String> errList = new ArrayList<>();
        errList.add(ex.getMessage());
        errList.add("\n----------------------------------------");
        errList.add("\nError processing Request: " + ex.getMessage());
        errList.add("\nInvalid URL:\n");
        errList.add(procUrl);
        LOG.log(Level.SEVERE, errList.toString());
        status = Vocabulary.CANCEL;
        return status;
    }

}
