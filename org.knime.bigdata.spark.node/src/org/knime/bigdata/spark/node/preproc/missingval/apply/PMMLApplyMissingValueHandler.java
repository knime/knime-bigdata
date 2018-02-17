/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 */
package org.knime.bigdata.spark.node.preproc.missingval.apply;

import java.io.Serializable;
import java.util.Map;

import org.dmg.pmml.DATATYPE;
import org.dmg.pmml.DerivedFieldDocument.DerivedField;
import org.knime.base.node.preproc.pmml.missingval.PMMLApplyMissingCellHandler;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverter;
import org.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandler;
import org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobInput;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.BooleanCell.BooleanCellFactory;
import org.knime.core.data.def.DoubleCell.DoubleCellFactory;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.IntCell.IntCellFactory;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.LongCell.LongCellFactory;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.time.localdate.LocalDateCellFactory;
import org.knime.core.data.time.localdatetime.LocalDateTimeCellFactory;
import org.knime.core.node.InvalidSettingsException;

/**
 * A missing value handler that is initialized from a PMML document instead of a factory.
 * Only works with the data types boolean, int, long, double, local date, local date time and string.
 *
 * Implementation based on {@link PMMLApplyMissingCellHandler}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class PMMLApplyMissingValueHandler extends SparkMissingValueHandler {

    private DerivedField m_derivedField;

    private String m_value;

    private DataType m_dataType;

    /**
     * @param col the column for which this handler is created
     * @param df the derived field that has the information for the missing value replacement
     * @throws InvalidSettingsException if the PMML structure cannot be interpreted
     */
    public PMMLApplyMissingValueHandler(final DataColumnSpec col, final DerivedField df)
            throws InvalidSettingsException {

        super(col);
        m_derivedField = df;
        try {
            m_value = df.getApply().getConstantList().get(0).getStringValue();
        } catch (NullPointerException e) {
            throw new InvalidSettingsException("The derived field for column " + col.getName()
                                               + " is malformed for missing value replacement", e);
        }

        DataType type = getKnimeDataTypeForPMML(df.getDataType());
        m_dataType = DataType.getCommonSuperType(type, col.getType());
    }

    @Override
    public Map<String, Serializable> getJobInputColumnConfig(final KNIMEToIntermediateConverter converter) throws InvalidSettingsException {
        return SparkMissingValueJobInput.createFixedValueConfig(converter.convert(getKnimeDataCell()));
    }

    @Override
    public DerivedField getPMMLDerivedField(final Object aggResult) {
        return m_derivedField;
    }

    @Override
    public DataType getOutputDataType() {
        return m_dataType;
    }

    /**
     * Maps the PMML value to a #{@link DataCell}.
     * @return a KNIME #{@link DataCell}.
     * @throws InvalidSettingsException on format exceptions
     */
    private DataCell getKnimeDataCell() throws InvalidSettingsException {
        try {
            if (m_dataType.equals(BooleanCell.TYPE)) {
                return BooleanCellFactory.create(m_value);
            } else if (m_dataType.equals(IntCell.TYPE)) {
                return IntCellFactory.create(m_value);
            } else if (m_dataType.equals(LongCell.TYPE)) {
                return LongCellFactory.create(m_value);
            } else if (m_derivedField.getDataType() == DATATYPE.DOUBLE) {
                return DoubleCellFactory.create(m_value);
            } else if (m_derivedField.getDataType() == DATATYPE.DATE) {
                return LocalDateCellFactory.create(m_value);
            } else if (m_derivedField.getDataType() == DATATYPE.DATE_TIME) {
                return LocalDateTimeCellFactory.create(m_value);
            }
        } catch (NumberFormatException e) {
            throw new InvalidSettingsException(
                "Could not parse PMML value of column " + getColumnSpec().getName() + ": " + m_value);
        }

        return new StringCell(m_value);
    }

    /**
     * Maps the PMML data type to a #{@link DataType}.
     * @return The data type for this handler's column
     */
    private DataType getKnimeDataTypeForPMML(final DATATYPE.Enum dt) {
        if (dt.equals(DATATYPE.DOUBLE)) {
            return DoubleCellFactory.TYPE;
        } else if (dt.equals(DATATYPE.BOOLEAN)) {
            return BooleanCellFactory.TYPE;
        } else if (dt.equals(DATATYPE.INTEGER)) {
            return IntCellFactory.TYPE;
        } else if (dt.equals(DATATYPE.DATE)) {
            return LocalDateCellFactory.TYPE;
        } else if (dt.equals(DATATYPE.DATE_TIME)) {
            return LocalDateTimeCellFactory.TYPE;
        } else  {
            return StringCell.TYPE;
        }
    }
}
