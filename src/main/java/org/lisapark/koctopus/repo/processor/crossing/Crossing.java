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
package org.lisapark.koctopus.repo.processor.crossing;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
import com.fasterxml.uuid.Generators;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.List;
import org.lisapark.koctopus.ProgrammerException;
import org.lisapark.koctopus.core.Persistable;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.event.Event;
import org.lisapark.koctopus.core.runtime.ProcessorContext;

import java.util.Map;
import java.util.UUID;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.memory.Memory;
import org.lisapark.koctopus.core.memory.MemoryProvider;
import org.lisapark.koctopus.core.processor.CompiledProcessor;
import org.lisapark.koctopus.core.processor.AbstractProcessor;
import org.lisapark.koctopus.core.processor.ProcessorInput;
import org.lisapark.koctopus.core.processor.ProcessorOutput;
import org.lisapark.koctopus.core.transport.TransportReference;
import org.lisapark.koctopus.util.Pair;
import org.lisapark.koctopus.core.transport.StreamingRuntime;

/**
 * This {@link AbstractProcessor} is used to determine if two SMAs are crossed.
 * 
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class Crossing extends AbstractProcessor<Pair> {
    
    private final static java.util.logging.Logger LOGGER 
            = java.util.logging.Logger.getLogger(Crossing.class.getName());
    
    private static final String DEFAULT_NAME = "Crossing";
    private static final String DEFAULT_DESCRIPTION = "Checks if crossing happened,"
            + " returns 1, if crossing is from above; -1, if crossing is from"
            + " under; 0 - otherwise.";

    /**
     * Crossing takes two inputs
     */
    private static final int FIRST_INPUT_ID = 1;
    private static final int SECOND_INPUT_ID = 2;
    private static final int OUTPUT_ID = 1;
    private static final int BUFFER_SIZE = 2;

    protected Crossing(UUID id, String name, String description) {
        super(id, name, description);
    }

    protected Crossing(UUID id, Crossing crossAboveToCopy) {
        super(id, crossAboveToCopy);
    }

    protected Crossing(Crossing crossAboveToCopy) {
        super(crossAboveToCopy);
    }

    public ProcessorInput getFirstInput() {
        // there are two inputs for crossAbove
        return getInputs().get(0);
    }

    public ProcessorInput getSecondInput() {
        // there are two inputs for crossAbove
        return getInputs().get(1);
    }

    @Override
    public Crossing newInstance() {
        return new Crossing(Generators.timeBasedGenerator().generate(), this);
    }

    @Override
    public Crossing newInstance(Gnode gnode) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Crossing copyOf() {
        return new Crossing(this);
    }
    
    /**
     * {@link CrossAbove}s need memory to store the prior events that will be used 
     * to determine if two SMAs are crossed. We
     * used a {@link MemoryProvider#createCircularBuffer(int)} to store this data.
     *
     * @param memoryProvider used to create CrosAbove's memory
     * @return circular buffer
     */
    @Override
    public Memory<Pair> createMemoryForProcessor(MemoryProvider memoryProvider) {
        return memoryProvider.createCircularBuffer(BUFFER_SIZE);
    }
    
    @Override
    public CompiledProcessor<Pair> compile() throws ValidationException {
        validate();

        // we copy all the inputs and output taking a "snapshot" of this processor so we are isolated of changes
        Crossing copy = copyOf();

        return new CompiledCrossing(copy);
    }

    /**
     * Returns a new {@link CrossAbove} processor configured with all the appropriate
     * {@link org.lisapark.koctopus.core.parameter.Parameter}s, {@link org.lisapark.koctopus.core.Input}s and {@link org.lisapark.koctopus.core.Output}.
     *
     * @return new {@link CrossAbove}
     */
    public static Crossing newTemplate() {
        UUID processorId = Generators.timeBasedGenerator().generate();
        Crossing cross = new Crossing(processorId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        // two double inputs
        ProcessorInput<Double> firstInput = ProcessorInput.doubleInputWithId(FIRST_INPUT_ID).name("Short SMA")
                .description("Short Simple Moving Average.").build();
        cross.addInput(firstInput);

        ProcessorInput<Double> secondInput = ProcessorInput.doubleInputWithId(SECOND_INPUT_ID).name("Long SMA")
                .description("Long Simple Moving Average.").build();
        cross.addInput(secondInput);

        cross.addJoin(firstInput, secondInput);

        // double output
        try {
            cross.setOutput(ProcessorOutput.booleanOutputWithId(OUTPUT_ID).nameAndDescription("Result")
                    .attributeName("isCrossed"));
        } catch (ValidationException ex) {
            // this should NOT happen. It means we created the CrossAbove with an invalid attribute name
            throw new ProgrammerException(ex);
        }

        return cross;
    }

    @Override
    public <T extends AbstractProcessor> CompiledProcessor<Pair> compile(T processor) throws ValidationException {
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

    static class CompiledCrossing extends CompiledProcessor<Pair> {
        private final String firstAttributeName;
        private final String secondAttributeName;

        protected CompiledCrossing(Crossing crossAbove) {
            super(crossAbove);

            firstAttributeName = crossAbove.getFirstInput().getSourceAttributeName();
            secondAttributeName = crossAbove.getSecondInput().getSourceAttributeName();
        }

        @Override
        public Object processEvent(ProcessorContext<Pair> ctx, Map<Integer, Event> eventsByInputId) {
            Event firstEvent = eventsByInputId.get(FIRST_INPUT_ID);
            Event secondEvent = eventsByInputId.get(SECOND_INPUT_ID);

            Double firstOperand = firstEvent.getAttributeAsDouble(firstAttributeName);
            Double secondOperand = secondEvent.getAttributeAsDouble(secondAttributeName);
            
            Integer retValue = 0;
            
            if (firstOperand != null && secondOperand != null) {
                
                Memory<Pair> processorMemory = ctx.getProcessorMemory();
                
                Pair<Double, Double> newPair = new Pair<>(firstOperand, secondOperand);
                processorMemory.add(newPair);
                
                List<Pair> list = Lists.newArrayList();

                final Collection<Pair> memoryItems = processorMemory.values();
                memoryItems.forEach((memoryItem) -> {
                    list.add(memoryItem);
                });

//logger.log(     Level.INFO, "list.add(memoryItem);{0}", list);
                
                if (list.size() >= BUFFER_SIZE) {
                  
                    Pair<Double, Double> firstPair = list.get(0);
                    Pair<Double, Double> secondPair = list.get(1);

                    if (firstPair.getFirst() > firstPair.getSecond()
                            && secondPair.getFirst() < secondPair.getSecond()) {
                        retValue = 1;
                    } else if(firstPair.getFirst() < firstPair.getSecond()
                            && secondPair.getFirst() > secondPair.getSecond()){
                        retValue = -1;
                    }
                }
            }            
            
            return retValue;
        }

        @Override
        public Object processEvent(StreamingRuntime runtime) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }
}
