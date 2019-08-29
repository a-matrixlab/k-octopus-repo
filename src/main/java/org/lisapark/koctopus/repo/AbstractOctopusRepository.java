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
import java.util.List;
import org.lisapark.koctopus.core.processor.AbstractProcessor;
import org.lisapark.koctopus.repo.processor.crossing.Crossing;
import org.lisapark.koctopus.repo.processor.forecast.ForecastSRM;
import org.lisapark.koctopus.repo.processor.regression.LinearRegressionProcessor;
import org.lisapark.koctopus.repo.processor.correlation.PearsonsCorrelationProcessor;
import org.lisapark.koctopus.repo.processor.sma.SmaRedis;
import org.lisapark.koctopus.repo.processor.sma.SmaOld;
import org.lisapark.koctopus.core.sink.external.ExternalSink;
import org.lisapark.koctopus.repo.sink.lucene.LuceneBaseIndex;
import org.lisapark.koctopus.repo.sink.DatabaseSink;
import org.lisapark.koctopus.repo.sink.ConsoleFromRedis;
import org.lisapark.koctopus.repo.source.DocDirSource;
import org.lisapark.koctopus.core.source.external.ExternalSource;
import org.lisapark.koctopus.repo.source.GdeltZipSource;
import org.lisapark.koctopus.repo.source.SqlQuerySource;
import org.lisapark.koctopus.repo.source.TestRandomBinarySource;
import org.lisapark.koctopus.repo.source.TestSourceRedis;

public abstract class AbstractOctopusRepository
        implements OctopusRepository {

    @Override
    public List<ExternalSink> getAllExternalSinkTemplates() {
        return Lists.newArrayList(new ExternalSink[]{
            ConsoleFromRedis.newTemplate(),
            LuceneBaseIndex.newTemplate(),
//            DatabaseSink.newTemplate()
        });
    }

    @Override
    public List<ExternalSource> getAllExternalSourceTemplates() {
        return Lists.newArrayList(new ExternalSource[]{
            DocDirSource.newTemplate(),
            //            KickStarterSource.newTemplate(),
//            GdeltZipSource.newTemplate(),
            //            RedisQuittokenSource.newTemplate(),
            //            RTCSource.newTemplate(),
//            SqlQuerySource.newTemplate(),
            //            TestSource.newTemplate(),
            TestSourceRedis.newTemplate(),
//            TestRandomBinarySource.newTemplate()
        });
    }

    @Override
    public List<AbstractProcessor> getAllProcessorTemplates() {
        return Lists.newArrayList(new AbstractProcessor[]{
//            Crossing.newTemplate(),
//            ForecastSRM.newTemplate(),
//            LinearRegressionProcessor.newTemplate(),
//            PearsonsCorrelationProcessor.newTemplate(),
            //            PipeDouble.newTemplate(),
            //            PipeString.newTemplate(),
            //            PipeStringDouble.newTemplate(),
            //            RTCcontroller.newTemplate(),
//            SmaOld.newTemplate(),
            SmaRedis.newTemplate()
        });
    }
}
