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
import java.util.logging.Level;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.memory.Memory;
import org.lisapark.koctopus.core.memory.MemoryProvider;
import org.lisapark.koctopus.core.processor.CompiledProcessor;
import org.lisapark.koctopus.core.processor.AbstractProcessor;
import org.lisapark.koctopus.core.processor.ProcessorInput;
import org.lisapark.koctopus.core.processor.ProcessorOutput;
import org.lisapark.koctopus.core.transport.TransportReference;
import org.lisapark.koctopus.util.Pair;
import org.lisapark.koctopus.core.transport.Transport;

/**
 * This {@link AbstractProcessor} is used to determine if two SMAs are crossed.
 *
 *
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class CrossUnder extends AbstractProcessor<Pair> {

    private final static java.util.logging.Logger LOGGER
            = java.util.logging.Logger.getLogger(CrossUnder.class.getName());

    private static final String DEFAULT_NAME = "Cross Under";
    private static final String DEFAULT_DESCRIPTION = "Checks if crossing from under happened";

    /**
     * CrossUnder takes two inputs
     */
    private static final int FIRST_INPUT_ID = 1;
    private static final int SECOND_INPUT_ID = 2;
    private static final int OUTPUT_ID = 1;
    private static final int BUFFER_SIZE = 3;

    protected CrossUnder(UUID id, String name, String description) {
        super(id, name, description);
    }

    protected CrossUnder(UUID id, CrossUnder crossUnderToCopy) {
        super(id, crossUnderToCopy);
    }

    protected CrossUnder(CrossUnder crossUnderToCopy) {
        super(crossUnderToCopy);
    }

    public ProcessorInput getFirstInput() {
        // there are two inputs for crossUnder
        return getInputs().get(0);
    }

    public ProcessorInput getSecondInput() {
        // there are two inputs for crossUnder
        return getInputs().get(1);
    }

    @Override
    public CrossUnder newInstance() {
        return new CrossUnder(Generators.timeBasedGenerator().generate(), this);
    }

    @Override
    public CrossUnder newInstance(Gnode gnode) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public CrossUnder copyOf() {
        return new CrossUnder(this);
    }

    /**
     * {@link CrossUnder}s need memory to store the prior events that will be
     * used to determine if two SMAs are crossed. We used a
     * {@link MemoryProvider#createCircularBuffer(int)} to store this data.
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
        CrossUnder copy = copyOf();

        return new CompiledCrossUnder(copy);
    }

    /**
     * Returns a new {@link CrossUnder} processor configured with all the
     * appropriate {@link org.lisapark.koctopus.core.parameter.Parameter}s,
     * {@link org.lisapark.koctopus.core.Input}s and
     * {@link org.lisapark.koctopus.core.Output}.
     *
     * @return new {@link CrossUnder}
     */
    public static CrossUnder newTemplate() {
        UUID processorId = Generators.timeBasedGenerator().generate();
        CrossUnder crossUnder = new CrossUnder(processorId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        // two double inputs
        ProcessorInput<Double> firstInput = ProcessorInput.doubleInputWithId(FIRST_INPUT_ID).name("Short SMA").description("Short Simple Moving Average.").build();
        crossUnder.addInput(firstInput);

        ProcessorInput<Double> secondInput = ProcessorInput.doubleInputWithId(SECOND_INPUT_ID).name("Long SMA").description("Long Simple Moving Average.").build();
        crossUnder.addInput(secondInput);

        crossUnder.addJoin(firstInput, secondInput);

        // double output
        try {
            crossUnder.setOutput(ProcessorOutput.doubleOutputWithId(OUTPUT_ID).nameAndDescription("Result").attributeName("CrossedFromUnder"));
        } catch (ValidationException ex) {
            // this should NOT happen. It means we created the CrossUnder with an invalid attribute name
            throw new ProgrammerException(ex);
        }

        return crossUnder;
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

    static class CompiledCrossUnder extends CompiledProcessor<Pair> {

        private final String firstAttributeName;
        private final String secondAttributeName;

        protected CompiledCrossUnder(CrossUnder crossUnder) {
            super(crossUnder);

            firstAttributeName = crossUnder.getFirstInput().getSourceAttributeName();
            secondAttributeName = crossUnder.getSecondInput().getSourceAttributeName();
        }

        @Override
        public Object processEvent(ProcessorContext<Pair> ctx, Map<Integer, Event> eventsByInputId) {
            Event firstEvent = eventsByInputId.get(FIRST_INPUT_ID);
            Event secondEvent = eventsByInputId.get(SECOND_INPUT_ID);

            Double firstOperand = firstEvent.getAttributeAsDouble(firstAttributeName);
            Double secondOperand = secondEvent.getAttributeAsDouble(secondAttributeName);

            Double retValue = 0D;

            if (firstOperand != null && secondOperand != null) {

                Memory<Pair> processorMemory = ctx.getProcessorMemory();

                LOGGER.info("Memory<Pair> processorMemory = ctx.getProcessorMemory();");

                Pair<Double, Double> newPair = new Pair<>(firstOperand, secondOperand);
                processorMemory.add(newPair);

                LOGGER.info("processorMemory.add(newPair);");

                List<Pair> list = Lists.newArrayList();

                final Collection<Pair> memoryItems = processorMemory.values();

                LOGGER.info("final Collection<Pair> memoryItems = processorMemory.values();");

                memoryItems.forEach((memoryItem) -> {
                    list.add(memoryItem);
                });

                LOGGER.log(Level.INFO, "list.add(memoryItem);{0}", list);

                if (list.size() >= BUFFER_SIZE) {

                    Pair<Double, Double> firstPair = list.get(0);
                    Pair<Double, Double> secondPair = list.get(2);

                    if (firstPair.getFirst() <= firstPair.getSecond()
                            && secondPair.getFirst() > secondPair.getSecond()) {
                        retValue = firstOperand;
                    }
                }
//                retValue = newPair.getFirst();
            }

            return retValue > 0 ? Boolean.TRUE : Boolean.FALSE;
        }

        @Override
        public Object processEvent(Transport runtime) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }
}
