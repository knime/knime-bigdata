/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Sep 11, 2018 (Mareike Höger): created
 */

package org.knime.bigdata.fileformats;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.TypeDescription;
import org.eclipse.core.runtime.Plugin;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCCellValueConsumerFactory;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCCellValueProducerFactory;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCDestination;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCListCellValueConsumerFactory;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCListCellValueProducerFactory;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCSource;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.core.data.convert.map.ConsumerRegistry;
import org.knime.core.data.convert.map.MappingFramework;
import org.knime.core.data.convert.map.ProducerRegistry;
import org.osgi.framework.BundleContext;

/**
 * Plugin for the File Format nodes
 * 
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 */
public class FileFormatPlugin extends Plugin {

    static final TypeDescription[] m_stringTypes = { TypeDescription.createString(), TypeDescription.createVarchar(),
            TypeDescription.createChar() };
    static final TypeDescription[] m_intTypes = { TypeDescription.createInt(), TypeDescription.createShort(),
            TypeDescription.createByte() };
    static final TypeDescription[] m_doubleTypes = { TypeDescription.createDouble(), TypeDescription.createFloat() };

    private static void registerORCConsumers() {

        final List<ORCCellValueConsumerFactory<?, ?>> primitiveConsumers = new ArrayList<>();

        for (final TypeDescription type : m_stringTypes) {
            final ORCCellValueConsumerFactory<String, BytesColumnVector> stringStringKindConsumer = new ORCCellValueConsumerFactory<>(
                    String.class, type, (cv, rowindex, v) -> {
                        final byte[] b = v.getBytes(StandardCharsets.UTF_8);
                        cv.ensureSize(b.length, true);
                        cv.setRef(rowindex, b, 0, b.length);
                    });
            primitiveConsumers.add(stringStringKindConsumer);
        }

        for (final TypeDescription type : m_intTypes) {
            final ORCCellValueConsumerFactory<Integer, LongColumnVector> integerIntegerConsumer = new ORCCellValueConsumerFactory<>(
                    Integer.class, type, (cv, rowindex, v) -> {
                        cv.vector[rowindex] = v;
                    });
            primitiveConsumers.add(integerIntegerConsumer);
        }
        for (final TypeDescription type : m_doubleTypes) {
            final ORCCellValueConsumerFactory<Double, DoubleColumnVector> doubleDoubleConsumer = new ORCCellValueConsumerFactory<>(
                    Double.class, type, (cv, rowindex, v) -> {
                        cv.vector[rowindex] = v;
                    });
            primitiveConsumers.add(doubleDoubleConsumer);
        }

        final ORCCellValueConsumerFactory<byte[], BytesColumnVector> byteArrayBinaryConsumer = new ORCCellValueConsumerFactory<>(
                byte[].class, TypeDescription.createBinary(), (cv, rowindex, v) -> {
                    cv.vector[rowindex] = v;
                });
        primitiveConsumers.add(byteArrayBinaryConsumer);

        final ORCCellValueConsumerFactory<Boolean, LongColumnVector> booleanBooleanConsumer = new ORCCellValueConsumerFactory<>(
                Boolean.class, TypeDescription.createBoolean(), (cv, rowindex, v) -> {
                    cv.vector[rowindex] = v ? 1 : 0;
                });
        primitiveConsumers.add(booleanBooleanConsumer);

        final ORCCellValueConsumerFactory<LocalDateTime, TimestampColumnVector> dateTimeTimestampConsumer = new ORCCellValueConsumerFactory<>(
                LocalDateTime.class, TypeDescription.createTimestamp(), (cv, rowindex, v) -> {
                    cv.set(rowindex, Timestamp.valueOf(v));
                });
        primitiveConsumers.add(dateTimeTimestampConsumer);


        final ORCCellValueConsumerFactory<String, DecimalColumnVector> stringBigDecimalConsumer = new ORCCellValueConsumerFactory<>(
                String.class, TypeDescription.createDecimal(), (cv, rowindex, v) -> {
                    final HiveDecimalWritable writeable = new HiveDecimalWritable(v);
                    if (writeable.isSet()) {
                        cv.vector[rowindex] = writeable;
                    } else {
                        throw new BigDataFileFormatException(String.format("Could not convert %s to decimal", v));
                    }

                });
        primitiveConsumers.add(stringBigDecimalConsumer);

        final ORCCellValueConsumerFactory<Byte, LongColumnVector> byteByteConsumer = new ORCCellValueConsumerFactory<>(Byte.class,
                TypeDescription.createByte(), (cv, rowindex, v) -> {
                    cv.vector[rowindex] = v.longValue();
                });
        primitiveConsumers.add(byteByteConsumer);

        final ORCCellValueConsumerFactory<Long, LongColumnVector> longLongConsumer = new ORCCellValueConsumerFactory<>(Long.class,
                TypeDescription.createLong(), (cv, rowindex, v) -> {
                    cv.vector[rowindex] = v;
                });
        primitiveConsumers.add(longLongConsumer);

        final ConsumerRegistry<TypeDescription, ORCDestination> consumerRegisty = MappingFramework
                .forDestinationType(ORCDestination.class);

        for (final ORCCellValueConsumerFactory<?, ?> elementFactory : primitiveConsumers) {
            consumerRegisty.register(elementFactory);
            final ORCListCellValueConsumerFactory<Array, ?, ?> listConsumer = new ORCListCellValueConsumerFactory<>(
                    elementFactory);
            consumerRegisty.register(listConsumer);
        }
    }

    private static void registerORCProducers() {
        final List<ORCCellValueProducerFactory<?, ?>> primitiveProducers = new ArrayList<>();

        final ORCCellValueProducerFactory<Boolean, LongColumnVector> booleanBooleanProducer = 
                new ORCCellValueProducerFactory<>(
                        TypeDescription.createBoolean(), Boolean.class, (columnVector, rowInBatchOrZero) -> {
                            final long valueL = columnVector.vector[rowInBatchOrZero];
                            return valueL == 1;
                        });
        primitiveProducers.add(booleanBooleanProducer);

        final ORCCellValueProducerFactory<Double, DoubleColumnVector> doubleDoubleProducer = 
                new ORCCellValueProducerFactory<>(
                        TypeDescription.createDouble(), Double.class, (columnVector, rowInBatchOrZero) -> {
                            return columnVector.vector[rowInBatchOrZero];
                        });
        primitiveProducers.add(doubleDoubleProducer);

        final ORCCellValueProducerFactory<Double, DoubleColumnVector> floatFloatProducer = 
                new ORCCellValueProducerFactory<>(
                        TypeDescription.createFloat(), Double.class, (columnVector, rowInBatchOrZero) -> {
                            return columnVector.vector[rowInBatchOrZero];

                        });
        primitiveProducers.add(floatFloatProducer);

        final ORCCellValueProducerFactory<Integer, LongColumnVector> intIntProducer = 
                new ORCCellValueProducerFactory<>(
                        TypeDescription.createInt(), Integer.class, (columnVector, rowInBatchOrZero) -> {
                            final long valueL = columnVector.vector[rowInBatchOrZero];
                            final int valueI = (int) valueL;
                            if (valueI != valueL) {
                                throw new BigDataFileFormatException(String.format(
                                        "Written as int but read as a long (overflow): (long)%d != (int)%d", 
                                        valueL, valueI));
                            }
                            return valueI;
                        });
        primitiveProducers.add(intIntProducer);

        final ORCCellValueProducerFactory<Long, LongColumnVector> longLongProducer = 
                new ORCCellValueProducerFactory<>(
                        TypeDescription.createLong(), Long.class, (columnVector, rowInBatchOrZero) -> {
                            return columnVector.vector[rowInBatchOrZero];

                        });
        primitiveProducers.add(longLongProducer);

        final ORCCellValueProducerFactory<LocalDateTime, TimestampColumnVector> timestampTimestampProducer = 
                new ORCCellValueProducerFactory<>(
                        TypeDescription.createTimestamp(), LocalDateTime.class, (columnVector, rowInBatchOrZero) -> {
                            return columnVector.asScratchTimestamp(rowInBatchOrZero).toLocalDateTime();

                        });
        primitiveProducers.add(timestampTimestampProducer);

        final ORCCellValueProducerFactory<LocalDate, LongColumnVector> localDateDateProducer =
                new ORCCellValueProducerFactory<>(
                        TypeDescription.createDate(), LocalDate.class, (columnVector, rowInBatchOrZero) -> {
                            final long valueL = columnVector.vector[rowInBatchOrZero];
                            return LocalDate.ofEpochDay(valueL);

                        });
        primitiveProducers.add(localDateDateProducer);

        final ORCCellValueProducerFactory<Integer, LongColumnVector> byteByteProducer = 
                new ORCCellValueProducerFactory<>(
                        TypeDescription.createByte(), Integer.class, (columnVector, rowInBatchOrZero) -> {
                            final long valueL = columnVector.vector[rowInBatchOrZero];
                            return (int) valueL;

                        });
        primitiveProducers.add(byteByteProducer);

        final ORCCellValueProducerFactory<Integer, LongColumnVector> intShortProducer = 
                new ORCCellValueProducerFactory<>(
                        TypeDescription.createShort(), Integer.class, (columnVector, rowInBatchOrZero) -> {
                            final long valueL = columnVector.vector[rowInBatchOrZero];
                            return (int) valueL;

                        });
        primitiveProducers.add(intShortProducer);

        final ORCCellValueProducerFactory<byte[], BytesColumnVector> binaryBinaryProducer = 
                new ORCCellValueProducerFactory<>(
                        TypeDescription.createBinary(), byte[].class, (columnVector, rowInBatchOrZero) -> {
                            return columnVector.vector[rowInBatchOrZero];
                        });
        primitiveProducers.add(binaryBinaryProducer);

        final ORCCellValueProducerFactory<String, DecimalColumnVector> bigDecimalDecimalProducer = 
                new ORCCellValueProducerFactory<>(
                        TypeDescription.createDecimal(), String.class, (columnVector, rowInBatchOrZero) -> {
                            return columnVector.vector[rowInBatchOrZero].getHiveDecimal().bigDecimalValue().toString();
                        });
        primitiveProducers.add(bigDecimalDecimalProducer);

        for (final TypeDescription type : m_stringTypes) {
            final ORCCellValueProducerFactory<String, BytesColumnVector> stringStringKindProducer = 
                    new ORCCellValueProducerFactory<>(
                            type, String.class, (columnVector, rowInBatchOrZero) -> {

                                return new String(columnVector.vector[rowInBatchOrZero], 
                                        columnVector.start[rowInBatchOrZero], columnVector.length[rowInBatchOrZero], 
                                        StandardCharsets.UTF_8);
                            });
            primitiveProducers.add(stringStringKindProducer);
        }

        // Register the specific producers
        final ProducerRegistry<TypeDescription, ORCSource> producerRegistry = MappingFramework
                .forSourceType(ORCSource.class);
        for (final ORCCellValueProducerFactory<?, ?> elementFactory : primitiveProducers) {
            producerRegistry.register(elementFactory);
            final ORCListCellValueProducerFactory<Array, ?, ?> collectionProducer = 
                    new ORCListCellValueProducerFactory<>(elementFactory);

            producerRegistry.register(collectionProducer);
        }
    }

    @Override
    public void start(final BundleContext context) throws Exception {
        registerORCProducers();
        registerORCConsumers();
    }

}
