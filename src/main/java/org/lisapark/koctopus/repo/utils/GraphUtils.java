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
package org.lisapark.koctopus.repo.utils;

import com.fasterxml.uuid.Generators;
import java.awt.Point;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.lisapark.koctopus.core.Input;
import org.lisapark.koctopus.core.Output;
import org.lisapark.koctopus.core.ProcessingModel;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.event.Attribute;
import org.lisapark.koctopus.core.graph.Constants;
import org.lisapark.koctopus.core.graph.Edge;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.graph.Graph;
import org.lisapark.koctopus.core.graph.NodeAttribute;
import org.lisapark.koctopus.core.graph.NodeAttributes;
import org.lisapark.koctopus.core.graph.NodeInput;
import org.lisapark.koctopus.core.graph.NodeInputs;
import org.lisapark.koctopus.core.graph.NodeOutput;
import org.lisapark.koctopus.core.graph.NodeParam;
import org.lisapark.koctopus.core.graph.NodeParams;
import org.lisapark.koctopus.core.graph.api.GraphVocabulary;
import org.lisapark.koctopus.core.graph.api.Vocabulary;
import org.lisapark.koctopus.core.parameter.Parameter;
import org.lisapark.koctopus.core.processor.AbstractProcessor;
import org.lisapark.koctopus.core.processor.ProcessorOutput;
import org.lisapark.koctopus.core.transport.TransportReference;
import org.lisapark.koctopus.core.sink.external.ExternalSink;
import org.lisapark.koctopus.core.source.Source;
import org.lisapark.koctopus.core.source.external.AbstractExternalSource;
import org.lisapark.koctopus.repo.OctopusRepository;

/**
 *
 * @author alexmy
 */
public class GraphUtils {

    static final Logger LOG = Logger.getLogger(GraphUtils.class.getName());
        
    public static final String OCTOPUS_RUNNER = "org.lisapark.koctopus.runner.OctopusRunner";

    /**
     *
     * @param source
     * @param gnode
     */
    public static void buildSource(AbstractExternalSource source, Gnode gnode) {
        NodeParams gparams = (NodeParams) gnode.getParams();
        Set<Parameter> params = source.getParameters();
        params.forEach((Parameter param) -> {
            NodeParam _param = gparams.getParams().get(param.getId());
            if (_param != null) {
                try {
                    String value = _param.getValue() == null ? null : _param.getValue().toString();
                    param.setValueFromString(value);
                } catch (ValidationException ex) {
                    LOG.log(Level.SEVERE, ex.getMessage());
                }
            }
        });
        final NodeOutput goutput = (NodeOutput) gnode.getOutput();
        Output output = source.getOutput();
        goutput.getAttributes().forEach((String name, NodeAttribute att) -> {
            if (output.getAttributeByName(name) == null) {
                Attribute newAttr;
                try {
                    newAttr = Attribute.newAttributeByClassName(att.getClassName(), name);
                    output.addAttribute(newAttr);
                } catch (ValidationException ex) {
                    LOG.log(Level.SEVERE, ex.getMessage());
                }
            }
        });
        source.setOutput(output);
    }

    /**
     *
     * @param sink
     * @param gnode
     */
    public static void buildSink(ExternalSink sink, Gnode gnode) {
        NodeParams gparams = (NodeParams) gnode.getParams();
        Set<Parameter> params = sink.getParameters();
        params.forEach((Parameter param) -> {
            NodeParam _param = (NodeParam) gparams.getParams().get(param.getId());
            if (_param != null) {
                try {
                    String value = _param.getValue() == null ? null : _param.getValue().toString();
                    param.setValueFromString(value);
                } catch (ValidationException ex) {
                    LOG.log(Level.SEVERE, ex.getMessage());
                }
            }
        });
        final NodeInputs ginputs = (NodeInputs) gnode.getInput();
        List<? extends Input> inputs = sink.getInputs();
        OctopusRepository repo = new OctopusRepository();
        inputs.forEach((Input input) -> {
            NodeInput _input = (NodeInput) ginputs.getSources().get(input.getName());
            if (_input != null) {
                try {
                    String sourceClassName = _input.getSourceClassName();
                    Object source = repo.getObjectByName(RepoUtils.getPair(sourceClassName));
                    if (source instanceof AbstractProcessor) {
                        AbstractProcessor proc = (AbstractProcessor) source;
                        proc.setId(UUID.fromString(_input.getSourceId()));
                        input.connectSource(proc);
                    } else if (source instanceof AbstractExternalSource) {
                        AbstractExternalSource exsource = (AbstractExternalSource) source;
                        exsource.setId(UUID.fromString(_input.getSourceId()));
                        input.connectSource(exsource);
                    }
                    TransportReference ref = new TransportReference();
                    ref.setReferenceClass(_input.getSourceClassName());
                    ref.setReferenceId(_input.getSourceId());
                    ref.setAttributes(_input.getAttributes());
                    sink.getReferences().put(input.getName(), ref);
                } catch (InstantiationException | IllegalAccessException | MalformedURLException ex) {
                    LOG.log(Level.SEVERE, ex.getMessage());
                }
            }
        });
    }

    /**
     *
     * @param processor
     * @param gnode
     */
    public static void buildProcessor(AbstractProcessor processor, Gnode gnode) {
        NodeParams gparams = (NodeParams) gnode.getParams();
        Set<Parameter> params = processor.getParameters();
        params.forEach((Parameter param) -> {
            NodeParam _param = (NodeParam) gparams.getParams().get(param.getId());
            if (_param != null) {
                try {
                    String value = _param.getValue() == null ? null : _param.getValue().toString();
                    param.setValueFromString(value);
                } catch (ValidationException ex) {
                    LOG.log(Level.SEVERE, ex.getMessage());
                }
            }
        });
        final NodeInputs ginputs = (NodeInputs) gnode.getInput();
        List<? extends Input> inputs = processor.getInputs();
        OctopusRepository repo = new OctopusRepository();
        inputs.forEach((Input input) -> {
            NodeInput _input = (NodeInput) ginputs.getSources().get(input.getName());
            if (_input != null) {
                try {
                    String sourceClassName = _input.getSourceClassName();
                    Object source = repo.getObjectByName(RepoUtils.getPair(sourceClassName));
                    if (source instanceof AbstractProcessor) {
                        AbstractProcessor proc = (AbstractProcessor) source;
                        proc.setId(UUID.fromString(_input.getSourceId()));
                        input.connectSource(proc);
                    } else if (source instanceof AbstractExternalSource) {
                        AbstractExternalSource exsource = (AbstractExternalSource) source;
                        exsource.setId(UUID.fromString(_input.getSourceId()));
                        input.connectSource(exsource);
                    }
                    TransportReference ref = new TransportReference();
                    ref.setReferenceClass(sourceClassName);
                    ref.setReferenceId(_input.getSourceId());
                    ref.setAttributes(_input.getAttributes());
                    processor.getReferences().put(input.getName(), ref);
                } catch (InstantiationException | IllegalAccessException | MalformedURLException ex) {
                    LOG.log(Level.SEVERE, ex.getMessage());
                }
            }
        });
        final NodeOutput goutput = (NodeOutput) gnode.getOutput();
        ProcessorOutput output = processor.getOutput();
        goutput.getAttributes().forEach((String name, NodeAttribute att) -> {
            if (output.getAttributeByName(name) == null) {
                Attribute newAttr;
                try {
                    newAttr = Attribute.newAttributeByClassName(att.getClassName(), name);
                    output.addAttribute(newAttr);
                } catch (ValidationException ex) {
                    LOG.log(Level.SEVERE, ex.getMessage());
                }
            }
        });
        processor.setOutput(output);
    }

    /**
     *
     * @param model
     * @param transportUrl
     * @return
     */
    public static Graph compileGraph(ProcessingModel model, String transportUrl) {
        return compileGraph(model, transportUrl, false);
    }

    /**
     *
     * @param model
     * @param transportUrl
     * @param resetUuid
     * @return
     */
    public static Graph compileGraph(ProcessingModel model, String transportUrl, boolean resetUuid) {
        Graph graph = new Graph();

        final Boolean  _resetUuid = false;
        
        UUID uuid = _resetUuid ? Generators.timeBasedGenerator().generate() : model.getId();
        graph.setId(uuid.toString());
        graph.setLabel(Vocabulary.MODEL);
        graph.setType(OCTOPUS_RUNNER);
        String _transportUrl;
        if (transportUrl == null) {
            if (model.getTransportUrl() == null) {
                _transportUrl = Constants.DEFAULT_TRANS_URL;
            } else {
                _transportUrl = model.getTransportUrl();
            }
        } else {
            _transportUrl = transportUrl;
        }
        String _serviceUrl = model.getServiceUrl();
        String _luceneIndex = model.getLuceneIndex();

        graph.setTransportUrl(_transportUrl);
        graph.setServiceUrl(_serviceUrl);
        graph.setLuceneIndex(_luceneIndex);

        graph.setColor(GraphVocabulary.UNTOUCHED);
        graph.setDirected(Boolean.TRUE);

        NodeParams gparams = new NodeParams();
        gparams.setParams(new HashMap<>());
        model.getParameters().forEach((param) -> {
            NodeParam _param = new NodeParam();
            _param.setId(param.getId());
            _param.setName(param.getName());
            _param.setClassName(param.getType().getCanonicalName());
            _param.setValue(param.getValue());
            gparams.getParams().put(param.getId(), _param);
        });
        graph.setParams(gparams);

        List<Gnode> nodes = new ArrayList<>();
        List<Edge> edges = new ArrayList<>();

        // Sources
        //======================================================================
        Set<AbstractExternalSource> sources = model.getExternalSources();
        sources.stream().forEach((AbstractExternalSource source) -> {
            Gnode sourceGnode = new Gnode();
            UUID uuid_1 = _resetUuid ? Generators.timeBasedGenerator().generate() : source.getId();
            sourceGnode.setId(uuid_1.toString());
            sourceGnode.setLabel(Vocabulary.SOURCE);
            sourceGnode.setType(source.getClass().getCanonicalName());
            sourceGnode.setTransportUrl(_transportUrl);
            sourceGnode.setX(source.getLocation().x);
            sourceGnode.setY(source.getLocation().y);

            Set<Parameter> params = source.getParameters();
            NodeParams _params = new NodeParams();
            _params.setParams(new HashMap<>());
            params.stream().forEach((Parameter param) -> {
                NodeParam _param = new NodeParam();
                _param.setId(param.getId());
                _param.setName(param.getName());
                _param.setClassName(param.getType().getCanonicalName());
                _param.setValue(param.getValue());
                _params.getParams().put(param.getId(), _param);
            });
            sourceGnode.setParams(_params);

            Output output = source.getOutput();
            List<Attribute> attrs = output.getAttributes();
            NodeOutput nodeOutput = new NodeOutput();
            nodeOutput.setName(output.getName());
            nodeOutput.setId(output.getId());

            NodeAttributes nodeattrs = new NodeAttributes();
            nodeattrs.setAttributes(new HashMap<>());
            attrs.stream().forEach((Attribute attr) -> {
                NodeAttribute nodeattr = new NodeAttribute();
                nodeattr.setName(attr.getName());
                nodeattr.setClassName(attr.getType().getCanonicalName());
                nodeattrs.getAttributes().put(attr.getName(), nodeattr);
            });
            nodeOutput.setAttributes(nodeattrs.getAttributes());
            sourceGnode.setOutput(nodeOutput);
            nodes.add(sourceGnode);
        });

        // Processors
        //======================================================================
        Set<AbstractProcessor> processors = model.getProcessors();
        processors.stream().forEach((AbstractProcessor proc) -> {
            Gnode procGnode = new Gnode();
            UUID uuid_2 = _resetUuid ? Generators.timeBasedGenerator().generate() : proc.getId();
            procGnode.setId(uuid_2.toString());
            procGnode.setLabel(Vocabulary.PROCESSOR);
            procGnode.setType(proc.getClass().getCanonicalName());
            procGnode.setTransportUrl(_transportUrl);
            procGnode.setX(proc.getLocation().x);
            procGnode.setY(proc.getLocation().y);

            // Setting params
            Set<Parameter> params = proc.getParameters();
            NodeParams _params = new NodeParams();
            _params.setParams(new HashMap<>());
            params.stream().forEach((Parameter param) -> {
                NodeParam _param = new NodeParam();
                _param.setId(param.getId());
                _param.setName(param.getName());
                _param.setClassName(param.getType().getCanonicalName());
                _param.setValue(param.getValue());
                _params.getParams().put(param.getId(), _param);
            });
            procGnode.setParams(_params);

            // Setting inputs
            List<? extends Input> inputs = proc.getInputs();
            NodeInputs nodeInputs = new NodeInputs();
            nodeInputs.setSources(new HashMap<>());
            inputs.stream().forEach((Input input) -> {
                Source inputSource = input.getSource();
                NodeInput nodeInput = new NodeInput();
                nodeInput.setAttributes(new HashMap());
                nodeInput.setId(input.getId());
                nodeInput.setName(input.getName());
                nodeInput.setSourceClassName(inputSource.getClass().getCanonicalName());
                nodeInput.setSourceId(inputSource.getId().toString());

                List<Attribute> attrs = inputSource.getOutput().getAttributes();
                NodeAttributes nodeattrs = new NodeAttributes();
                nodeattrs.setAttributes(new HashMap<>());
                attrs.stream().forEach((Attribute attr) -> {
                    NodeAttribute nodeattr = new NodeAttribute();
                    nodeattr.setClassName(attr.getType().getCanonicalName());
                    nodeattr.setName(attr.getName());
                    nodeattrs.getAttributes().put(attr.getName(), nodeattr);
                });
                nodeInput.setAttributes(nodeattrs.getAttributes());
                nodeInputs.getSources().put(input.getName(), nodeInput);
            });
            procGnode.setInput(nodeInputs);

            Output output = proc.getOutput();
            List<Attribute> attrs = output.getAttributes();
            NodeOutput nodeOutput = new NodeOutput();
            nodeOutput.setName(output.getName());
            nodeOutput.setId(output.getId());

            NodeAttributes nodeattrs = new NodeAttributes();
            nodeattrs.setAttributes(new HashMap<>());
            attrs.stream().forEach((Attribute attr) -> {
                NodeAttribute nodeattr = new NodeAttribute();
                nodeattr.setName(attr.getName());
                nodeattr.setClassName(attr.getType().getCanonicalName());
                nodeattrs.getAttributes().put(attr.getName(), nodeattr);
            });
            nodeOutput.setAttributes(nodeattrs.getAttributes());
            procGnode.setOutput(nodeOutput);
            nodes.add(procGnode);
        });

        // Sinks
        //======================================================================
        Set<ExternalSink> sinks = model.getExternalSinks();
        sinks.stream().forEach((ExternalSink sink) -> {
            Gnode sinkGnode = new Gnode();
            UUID uuid_3 = _resetUuid ? Generators.timeBasedGenerator().generate() : sink.getId();
            sinkGnode.setId(uuid_3.toString());
            sinkGnode.setLabel(Vocabulary.SINK);
            sinkGnode.setType(sink.getClass().getCanonicalName());
            sinkGnode.setTransportUrl(_transportUrl);
            sinkGnode.setX(sink.getLocation().x);
            sinkGnode.setY(sink.getLocation().y);

            Set<Parameter> params = sink.getParameters();
            NodeParams _params = new NodeParams();
            _params.setParams(new HashMap<>());
            params.stream().forEach((Parameter param) -> {
                NodeParam _param = new NodeParam();
                _param.setId(param.getId());
                _param.setName(param.getName());
                _param.setClassName(param.getType().getCanonicalName());
                _param.setValue(param.getValue());
                _params.getParams().put(param.getId(), _param);
            });
            sinkGnode.setParams(_params);

            // Setting inputs
            List<? extends Input> inputs = sink.getInputs();
            NodeInputs nodeInputs = new NodeInputs();
            nodeInputs.setSources(new HashMap<>());
            inputs.stream().forEach((Input input) -> {
                Source inputSource = input.getSource();
                NodeInput nodeInput = new NodeInput();
                nodeInput.setAttributes(new HashMap());
                nodeInput.setId(input.getId());
                nodeInput.setName(input.getName());
                nodeInput.setSourceClassName(inputSource.getClass().getCanonicalName());
                nodeInput.setSourceId(inputSource.getId().toString());

                List<Attribute> attrs = inputSource.getOutput().getAttributes();
                NodeAttributes nodeattrs = new NodeAttributes();
                nodeattrs.setAttributes(new HashMap<>());
                attrs.stream().forEach((Attribute attr) -> {
                    NodeAttribute nodeattr = new NodeAttribute();
                    nodeattr.setClassName(attr.getType().getCanonicalName());
                    nodeattr.setName(attr.getName());
                    nodeattrs.getAttributes().put(attr.getName(), nodeattr);
                });
                nodeInput.setAttributes(nodeattrs.getAttributes());
                nodeInputs.getSources().put(input.getName(), nodeInput);
            });
            sinkGnode.setInput(nodeInputs);
            nodes.add(sinkGnode);
        });
        graph.setNodes(nodes);
        // Build node id lookup map
//        Map<String, String> lookup = new HashMap<>();
//        graph.getNodes().forEach((node) -> {
//            lookup.put(node.getType(), node.getId());
//        });
        // Create all edges
        graph.getNodes().forEach(node -> {
            if (Vocabulary.SOURCE.equalsIgnoreCase(node.getLabel())) {
                // Do nothing, source has no in-edges
            } else {
                node.getInput().getSources().forEach((k, input) -> {
                    // Create edges
                    Edge edge = new Edge();
                    edge.setLabel(Vocabulary.MODEL);
                    edge.setRelation(input.getName());
                    edge.setDirected(true);

//                    String sourceNodeId = lookup.get(input.getSourceClassName());
//                    input.setSourceId(sourceNodeId);
                    edge.setSource(input.getSourceClassName() + ":" + input.getSourceId());
                    edge.setTarget(node.getType() + ":" + node.getId());
                    edges.add(edge);
                });
            }
        });
        graph.setEdges(edges);

        return graph;
    }

    /**
     *
     * @param graph
     * @param modelName
     * @return
     */
    public static ProcessingModel buildProcessingModel(Graph graph, String modelName) {
        String transUrl = graph.getTransportUrl();
        ProcessingModel model = new ProcessingModel(modelName, transUrl);
        Set<Parameter> params = model.getParameters();
        params.forEach((Parameter param) -> {
            NodeParam _param = (NodeParam) graph.getParams().getParams().get(param.getId());
            if (_param != null) {
                try {
                    String value = _param.getValue() == null ? null : _param.getValue().toString();
                    param.setValueFromString(value);
                } catch (ValidationException ex) {
                    LOG.log(Level.SEVERE, ex.getMessage());
                }
            }
        });
        // Node's lookup map
        Map<String, Object> lookupModel = new HashMap<>();
        OctopusRepository repo = new OctopusRepository();
        graph.getNodes().forEach((gnode) -> {
            try {
                String type;
                switch (gnode.getLabel()) {
                    case Vocabulary.SOURCE:
                        type = gnode.getType();
                        AbstractExternalSource sourceIns = (AbstractExternalSource) repo.getAbstractExternalSourceByName(RepoUtils.getPair(type)); 
                        AbstractExternalSource source = (AbstractExternalSource) sourceIns.newInstance(gnode);
                        source.setLocation(new Point(gnode.getX(), gnode.getY()));
                        model.addExternalEventSource(source);
                        lookupModel.put(source.getId().toString(), source);
                        break;
                    case Vocabulary.PROCESSOR:
                        type = gnode.getType();
                        AbstractProcessor processorIns = (AbstractProcessor) repo.getAbstractProcessorByName(RepoUtils.getPair(type));
                        AbstractProcessor processor = (AbstractProcessor) processorIns.newInstance(gnode);
                        processor.setLocation(new Point(gnode.getX(), gnode.getY()));
                        lookupModel.put(processor.getId().toString(), processor);
                        model.addProcessor(processor);
                        break;
                    case Vocabulary.SINK:
                        type = gnode.getType();
                        ExternalSink sinkIns = (ExternalSink) repo.getAbstractExternalSinkByName(RepoUtils.getPair(type)); 
                        ExternalSink sink = (ExternalSink) sinkIns.newInstance(gnode);
                        sink.setLocation(new Point(gnode.getX(), gnode.getY()));
                        lookupModel.put(sink.getId().toString(), sink);
                        model.addExternalSink(sink);
                        break;
                    default:
                        break;
                }
            } catch (InstantiationException | IllegalAccessException | MalformedURLException ex) {
                LOG.log(Level.SEVERE, ex.getMessage());
            }
        });
        // Set all connections for processor and sink nodes inputs
        model.getProcessors().forEach((AbstractProcessor proc) -> {
            List<? extends Input> inputs = proc.getInputs();
            inputs.forEach((input) -> {
                if (input != null) {
                    String uuid = input.getSource().getId().toString();
                    if (lookupModel.get(uuid) instanceof Source) {
                        input.connectSource((Source) lookupModel.get(uuid));
                    } else {
                        input.connectSource((AbstractProcessor) lookupModel.get(uuid));
                    }
                }
            });
        });
        model.getExternalSinks().forEach((ExternalSink sink) -> {
            List<? extends Input> inputs = sink.getInputs();
            inputs.forEach((Input input) -> {
                if (input != null) {
                    String uuid = input.getSource().getId().toString();
                    if (lookupModel.get(uuid) instanceof Source) {
                        input.connectSource((Source) lookupModel.get(uuid));
                    } else {
                        input.connectSource((AbstractProcessor) lookupModel.get(uuid));
                    }
                }
            });
        });
        return model;
    }

    /**
     *
     * @param graphJson
     * @param path
     * @return
     */
    public static Document graphLuceneDoc(String graphJson, String path) {
        Graph graph = new Graph().fromJson(graphJson);
        return graphLuceneDoc(graph, path);
    }

    /**
     *
     * @param graph
     * @param path
     * @return
     */
    public static Document graphLuceneDoc(Graph graph, String path) {
        Document graphDoc = new Document();
        FieldType meta = typeMeta();
        FieldType text = typeText();

        graphDoc.add(new Field("id", graph.getId(), meta));

        return graphDoc;
    }

    private static FieldType typeText() {
        // This is the field setting for normal text field.
        FieldType text = new FieldType();
        text.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        text.setStoreTermVectors(true);
        text.setStoreTermVectorPositions(true);
        text.setTokenized(true);
        text.setStored(true);
        text.freeze();

        return text;
    }

    private static FieldType typeMeta() {
        // This is the field setting for metadata field.
        FieldType meta = new FieldType();
        meta.setOmitNorms(true);
        meta.setIndexOptions(IndexOptions.DOCS);
        meta.setStored(true);
        meta.setTokenized(false);
        meta.freeze();
        return meta;
    }

    public static Document graphNodeLuceneDoc(Gnode gnode) {
        Document gnodeDoc = new Document();

        return gnodeDoc;
    }
}
