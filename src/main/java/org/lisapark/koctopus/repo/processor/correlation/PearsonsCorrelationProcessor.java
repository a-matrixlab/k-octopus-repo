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
package org.lisapark.koctopus.repo.processor.correlation;

import com.fasterxml.uuid.Generators;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.lisapark.koctopus.ProgrammerException;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.event.Event;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.memory.Memory;
import org.lisapark.koctopus.core.memory.MemoryProvider;
import org.lisapark.koctopus.core.parameter.Constraints;
import org.lisapark.koctopus.core.parameter.Parameter;
import org.lisapark.koctopus.core.processor.CompiledProcessor;
import org.lisapark.koctopus.core.processor.AbstractProcessor;
import org.lisapark.koctopus.core.processor.ProcessorInput;
import org.lisapark.koctopus.core.processor.ProcessorOutput;
import org.lisapark.koctopus.core.runtime.ProcessorContext;
import org.lisapark.koctopus.core.transport.TransportReference;
import org.lisapark.koctopus.util.Pair;
import org.lisapark.koctopus.core.transport.StreamingRuntime;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class PearsonsCorrelationProcessor extends AbstractProcessor<Pair<Double, Double>> {
    
    private final static java.util.logging.Logger LOGGER 
            = java.util.logging.Logger.getLogger(PearsonsCorrelationProcessor.class.getName());
    
//    private static final String DEFAULT_NAME = "PearsonsCorrelation";
//    private static final String DEFAULT_DESCRIPTION = "Calculate Pearsons correlation for two series ";
//    private static final String DEFAULT_WINDOW_LENGTH_DESCRIPTION = "Number of data points to consider when performing the correlation.";
    
    private static final String DEFAULT_NAME = "PearsonCorrelation";
    private static final String DEFAULT_DESCRIPTION = "Pearson Correlation. ";
    private static final String DEFAULT_WINDOW_LENGTH_DESCRIPTION = "Time window, size of the arrays should be the same for both sequences.";
    private static final String DEFAULT_OUTPUT_DESCRIPTION = "Pearson correlation coefficient.";

    /**
     * PearsonsCorrelationProcessor takes two inputs
     */
    private static final int FIRST_INPUT_ID = 1;
    private static final int SECOND_INPUT_ID = 2;
    private static final int WINDOW_LENGTH_PARAMETER_ID = 3;
    
    private static final int OUTPUT_ID = 1;
    

    protected PearsonsCorrelationProcessor(UUID id, String name, String description) {
        super(id, name, description);
    }

    protected PearsonsCorrelationProcessor(UUID id, PearsonsCorrelationProcessor correlationToCopy) {
        super(id, correlationToCopy);
    }

    protected PearsonsCorrelationProcessor(PearsonsCorrelationProcessor correlationToCopy) {
        super(correlationToCopy);
    }

    
    public int getWindowLength() {
        return getParameter(WINDOW_LENGTH_PARAMETER_ID).getValueAsInteger();
    }

    @SuppressWarnings("unchecked")
    public void setWindowLength(int windowLength) throws ValidationException {
        getParameter(WINDOW_LENGTH_PARAMETER_ID).setValue(windowLength);
    }
    
    public ProcessorInput getFirstInput() {
        // there are two inputs for pearsonsCorrelationProcessor
        return getInputs().get(0);
    }

    public ProcessorInput getSecondInput() {
        // there are two inputs for pearsonsCorrelationsProcessor
        return getInputs().get(1);
    }

    @Override
    public PearsonsCorrelationProcessor newInstance() {
        return new PearsonsCorrelationProcessor(Generators.timeBasedGenerator().generate(), this);
    }

    @Override
    public PearsonsCorrelationProcessor newInstance(Gnode gnode) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public PearsonsCorrelationProcessor copyOf() {
        return new PearsonsCorrelationProcessor(this);
    }
    
    /**
     * {@link Correlation}s need memory to store the prior events that will be used 
     * to calculate Pearsons correlation. We
     * used a {@link MemoryProvider#createCircularBuffer(int)} to store this data.
     *
     * @param memoryProvider used to create Correlation's memory
     * @return circular buffer
     */
    @Override
    public Memory<Pair<Double, Double>> createMemoryForProcessor(MemoryProvider memoryProvider) {
        return memoryProvider.createCircularBuffer(getWindowLength());
    }
    
    @Override
    public CompiledProcessor<Pair<Double, Double>> compile() throws ValidationException {
        validate();

        // we copy all the inputs and output taking a "snapshot" of this processor so we are isolated of changes
        PearsonsCorrelationProcessor copy = copyOf();

        return new CompiledCorrelation(copy);
    }

    /**
     * Returns a new {@link PearsonsCorrelationProcessor} processor configured with all the appropriate
     * {@link org.lisapark.koctopus.core.parameter.Parameter}s, {@link org.lisapark.koctopus.core.Input}s and {@link org.lisapark.koctopus.core.Output}.
     *
     * @return new {@link PearsonsCorrelationProcessor}
     */
    public static PearsonsCorrelationProcessor newTemplate() {
        UUID processorId = Generators.timeBasedGenerator().generate();
        PearsonsCorrelationProcessor correlation = new PearsonsCorrelationProcessor(processorId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        
        correlation.addParameter(
                Parameter.integerParameterWithIdAndName(WINDOW_LENGTH_PARAMETER_ID, "Time window").
                        description(DEFAULT_WINDOW_LENGTH_DESCRIPTION).
                        defaultValue(10).required(true).
                        constraint(Constraints.integerConstraintWithMinimumAndMessage(1, "Time window should be greater than 0."))
        );

        // two double inputs
        ProcessorInput<Double> firstInput = ProcessorInput.doubleInputWithId(FIRST_INPUT_ID).name("Sequence 1").description("Sequence 1.").build();
        correlation.addInput(firstInput);

        ProcessorInput<Double> secondInput = ProcessorInput.doubleInputWithId(SECOND_INPUT_ID).name("Sequence 2").description("Sequence 2.").build();
        correlation.addInput(secondInput);

        correlation.addJoin(firstInput, secondInput);

        // double output
        try {
            correlation.setOutput(ProcessorOutput.doubleOutputWithId(OUTPUT_ID).name("Correlation").attributeName("PearsonsCorrelationCoefficient").description(DEFAULT_OUTPUT_DESCRIPTION));
        } catch (ValidationException ex) {
            // this should NOT happen. It means we created the Addition with an invalid attribute name
            throw new ProgrammerException(ex);
        }

        return correlation;
    }

    @Override
    public <T extends AbstractProcessor> CompiledProcessor<Pair<Double, Double>> compile(T processor) throws ValidationException {
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

    static class CompiledCorrelation extends CompiledProcessor<Pair<Double, Double>> {
        private final String firstAttributeName;
        private final String secondAttributeName;
        
        private final PearsonsCorrelationProcessor correlation;

        protected CompiledCorrelation(PearsonsCorrelationProcessor correlation) {
            super(correlation);
            this.correlation = correlation;

            firstAttributeName = correlation.getFirstInput().getSourceAttributeName();
            secondAttributeName = correlation.getSecondInput().getSourceAttributeName();
        }

        @Override
        public Object processEvent(ProcessorContext<Pair<Double, Double>> ctx, Map<Integer, Event> eventsByInputId) {
            Event firstEvent = eventsByInputId.get(FIRST_INPUT_ID);
            Event secondEvent = eventsByInputId.get(SECOND_INPUT_ID);

            Double firstOperand = firstEvent.getAttributeAsDouble(firstAttributeName);
            Double secondOperand = secondEvent.getAttributeAsDouble(secondAttributeName);
            
            Double retValue = null;
            
            if (firstOperand != null && secondOperand != null) {
                
                Memory<Pair<Double, Double>> processorMemory = ctx.getProcessorMemory();
                
                Pair<Double, Double> newPair = new Pair<>(firstOperand, secondOperand);
                processorMemory.add(newPair);
               
                int arraySize = correlation.getWindowLength(); 
                double[] first = new double[arraySize];
                double[] second = new double[arraySize];

                final Collection<Pair<Double, Double>> memoryItems = processorMemory.values();
               
                if (memoryItems.size() >= arraySize) {

                    int i = 0;
                    for (Pair<Double, Double> memoryItem : memoryItems) {
                        first[i] = memoryItem.getFirst();
                        second[i] = memoryItem.getSecond();
                        i++;
                    }
                    
                    retValue = new PearsonsCorrelation().correlation(first, second);

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
