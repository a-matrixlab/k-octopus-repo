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
package org.lisapark.koctopus.repo.pipe;

import com.fasterxml.uuid.Generators;
import java.util.Map;
import java.util.UUID;
import org.lisapark.koctopus.ProgrammerException;
import org.lisapark.koctopus.core.Input;
import org.lisapark.koctopus.core.Output;
import org.lisapark.koctopus.core.Persistable;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.event.Event;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.processor.CompiledProcessor;
import org.lisapark.koctopus.core.processor.AbstractProcessor;
import org.lisapark.koctopus.core.processor.ProcessorInput;
import org.lisapark.koctopus.core.processor.ProcessorOutput;
import org.lisapark.koctopus.core.runtime.ProcessorContext;
import org.lisapark.koctopus.core.transport.TransportReference;
import org.lisapark.koctopus.core.transport.StreamingRuntime;

/**
 * This {@link AbstractProcessor} is used for transferring Double value from one processor to another.
 * 
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 * @author Alex Mylnikov (alexmy@lisa-park.com) mylnikov(alexmy@lisa-park.com)
 */
@Persistable
public class PipeString extends AbstractProcessor<Double> {
    
    private static final String DEFAULT_NAME = "Connector String";
    private static final String DEFAULT_DESCRIPTION = "Transfere string data from one processor to another.";
//    private static final String DEFAULT_WINDOW_LENGTH_DESCRIPTION = "Количество наблюдаемых значений";
    private static final String DEFAULT_INPUT_DESCRIPTION = "Input data";
    private static final String DEFAULT_OUTPUT_DESCRIPTION = "Output data";

    /**
     * Pipe takes a single input
     */
    private static final int INPUT_ID = 1;
    private static final int OUTPUT_ID = 1;

    protected PipeString(UUID id, String name, String description) {
        super(id, name, description);
    }

    protected PipeString(UUID id, PipeString copyFromSma) {
        super(id, copyFromSma);
    }

    protected PipeString(PipeString copyFromSma) {
        super(copyFromSma);
    }

    public ProcessorInput getInput() {
        // there is only one input for an Sma
        return getInputs().get(0);
    }

    @Override
    public PipeString newInstance() {
        return new PipeString(Generators.timeBasedGenerator().generate(), this);
    }

    @Override
    public PipeString newInstance(Gnode gnode) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public PipeString copyOf() {
        return new PipeString(this);
    }

    /**
     * Validates and compile this Pipe.Doing so takes a "snapshot" of the {@link #getInputs()} and {@link #output}
     * and returns a {@link CompiledProcessor}.
     *
     * @return CompiledProcessor
     * @throws org.lisapark.koctopus.core.ValidationException
     */
    @Override
    public CompiledProcessor<Double> compile() throws ValidationException {
        validate();

        // we copy all the inputs and output taking a "snapshot" of this processor so we are isolated of changes
        PipeString copy = copyOf();
        return new CompiledSma(copy);
    }

    /**
     * Returns a new {@link Sma} processor configured with all the appropriate {@link org.lisapark.koctopus.core.parameter.Parameter}s, {@link Input}s
     * and {@link Output}.
     *
     * @return new {@link Sma}
     */
    public static PipeString newTemplate() {
        UUID processorId = Generators.timeBasedGenerator().generate();
        PipeString connector = new PipeString(processorId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        // only a single double input
        connector.addInput(
                ProcessorInput.stringInputWithId(INPUT_ID).name("Input data").description(DEFAULT_INPUT_DESCRIPTION)
        );
        // double output
        try {
            connector.setOutput(
                    ProcessorOutput.stringOutputWithId(OUTPUT_ID).name("Output").description(DEFAULT_OUTPUT_DESCRIPTION).attributeName("output")
            );
        } catch (ValidationException ex) {
            // this should NOT happen. It means we created the SMA with an invalid attriubte name
            throw new ProgrammerException(ex);
        }

        return connector;
    }

    @Override
    public <T extends AbstractProcessor> CompiledProcessor<Double> compile(T processor) throws ValidationException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Map<String, TransportReference> getReferences() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setReferences(Map<String, TransportReference> sourceref) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * This {@link CompiledProcessor} is the actual logic that implements the Simple Moving Average.
     */
    static class CompiledSma extends CompiledProcessor<Double> {
        private final String inputAttributeName;

        protected CompiledSma(PipeString pipe) {
            super(pipe);
            this.inputAttributeName = pipe.getInput().getSourceAttributeName();
        }

        @Override
        public Object processEvent(ProcessorContext<Double> ctx, Map<Integer, Event> eventsByInputId) {
            // sma only has a single event
            Event event = eventsByInputId.get(INPUT_ID);

            String newItem = event.getAttributeAsString(inputAttributeName);

            if (newItem == null) {
                newItem = "";
            }
            
            return newItem;
        }

        @Override
        public Object processEvent(StreamingRuntime runtime) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }
}
