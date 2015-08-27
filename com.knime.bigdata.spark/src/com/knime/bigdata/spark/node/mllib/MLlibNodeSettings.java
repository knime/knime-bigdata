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
 *
 * History
 *   Created on 23.08.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.dmg.pmml.DerivedFieldDocument.DerivedField;
import org.dmg.pmml.InlineTableDocument.InlineTable;
import org.dmg.pmml.MapValuesDocument.MapValues;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter2;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnFilter2;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;

import com.knime.bigdata.spark.jobserver.server.NominalFeatureInfo;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.util.SparkUtil;

/**
 * Settings class that contains commonly used settings required to learn a Spark MLlib model such as
 * class column and feature columns. It also provides helper method to be used in the NodeModel and NodeDialog.
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibNodeSettings {

    private final SettingsModelString m_classCol = createClassColModel();

    private final SettingsModelColumnFilter2 m_cols = createColumnsModel();

    private boolean m_requiresClassCol;

    /**
     * @param requiresClassCol <code>true</code> if the class column name is mandatory
     *
     */
    public MLlibNodeSettings(final boolean requiresClassCol) {
        m_requiresClassCol = requiresClassCol;
    }

    /**
     * @return the class column model
     */
    public static SettingsModelString createClassColModel() {
        return new SettingsModelString("classColumn", null);
    }

    /**
     * @return the feature column model
     */
    @SuppressWarnings("unchecked")
    public static SettingsModelColumnFilter2 createColumnsModel() {
        return new SettingsModelColumnFilter2("featureColumns", DoubleValue.class);
    }

    /**
     * @param tableSpec the original input {@link DataTableSpec}
     * @throws InvalidSettingsException  if the settings are invalid
     */
    public void check(final DataTableSpec tableSpec) throws InvalidSettingsException {
        final String classColName = m_classCol.getStringValue();
        if (m_requiresClassCol && classColName == null) {
            throw new InvalidSettingsException("No class column selected");
        }
        final int classColIdx = tableSpec.findColumnIndex(classColName);
        if (m_requiresClassCol && classColIdx < 0) {
            throw new InvalidSettingsException("Class column :" + classColName + " not found in input data");
        }
        final FilterResult result = m_cols.applyTo(tableSpec);
        final List<String> featureColNames =  Arrays.asList(result.getIncludes());
        Integer[] featureColIdxs = SparkUtil.getColumnIndices(tableSpec, featureColNames);
        if (Arrays.asList(featureColIdxs).contains(Integer.valueOf(classColIdx))) {
            throw new InvalidSettingsException("Class column '" + classColName + "' also selected as feature column");
        }
    }

    /**
     * @param settings the {@link NodeSettingsWO} to write to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_classCol.saveSettingsTo(settings);
        m_cols.saveSettingsTo(settings);
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @throws InvalidSettingsException  if the settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (m_requiresClassCol) {
            m_classCol.validateSettings(settings);
            final String classCol =
                    ((SettingsModelString)m_classCol.createCloneWithValidatedValue(settings)).getStringValue();
            if (classCol == null || classCol.isEmpty()) {
                throw new InvalidSettingsException("Class column must not be empty");
            }
        }
        m_cols.validateSettings(settings);
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @throws InvalidSettingsException if the settings are invalid
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (m_requiresClassCol) {
            m_classCol.loadSettingsFrom(settings);
        }
        m_cols.loadSettingsFrom(settings);
    }


    /**
     * @param rdd the {@link SparkDataPortObject}
     * @return the {@link MLlibSettings} object for the input table spec
     * @throws InvalidSettingsException if the settings are invalid
     */
    public MLlibSettings getSettings(final SparkDataPortObject rdd) throws InvalidSettingsException {
        return getSettings(rdd.getTableSpec(), null);
    }
    /**
     * @param tableSpec the original input {@link DataTableSpec}
     * @return the {@link MLlibSettings} object for the input table spec
     * @throws InvalidSettingsException if the settings are invalid
     */
    public MLlibSettings getSettings(final DataTableSpec tableSpec) throws InvalidSettingsException {
        return getSettings(tableSpec, null);
    }


    /**
     * @param rdd {@link SparkDataPortObject}
     * @param mapping {@link PMMLPortObject} with the category to number mapping info or <code>null</code> if
     * not available
     * @return the {@link MLlibSettings} object for the input table spec
     * @throws InvalidSettingsException if the settings are invalid
     */
    public MLlibSettings getSettings(final SparkDataPortObject rdd, final PMMLPortObject mapping) throws InvalidSettingsException {
        return getSettings(rdd.getTableSpec(), mapping);
    }
    /**
     * @param tableSpec the original input {@link DataTableSpec}
     * @param mapping {@link PMMLPortObject} with the category to number mapping info or <code>null</code> if
     * not available
     * @return the {@link MLlibSettings} object for the input table spec
     * @throws InvalidSettingsException if the settings are invalid
     */
    public MLlibSettings getSettings(final DataTableSpec tableSpec, final PMMLPortObject mapping) throws InvalidSettingsException {
        final String classColName = m_classCol.getStringValue();
        final int classColIdx = tableSpec.findColumnIndex(classColName);
        if (m_requiresClassCol && classColIdx < 0) {
            throw new InvalidSettingsException("Class column :" + classColName + " not found in input data");
        }
        final FilterResult filterResult = m_cols.applyTo(tableSpec);
        final List<String> featureColNames = Arrays.asList(filterResult.getIncludes());
        final Integer[] featureColIdxs = SparkUtil.getColumnIndices(tableSpec, featureColNames);
        if (Arrays.asList(featureColIdxs).contains(Integer.valueOf(classColIdx))) {
            throw new InvalidSettingsException("Class column also selected as feature column");
        }

        //PMML mapping info if available
        final Long numberOfClasses;
        final NominalFeatureInfo featureInfo;
        if (mapping != null) {
            final Map<String, DerivedField> mapValues = MLlibNodeSettings.getMapValues(mapping);
             numberOfClasses = MLlibNodeSettings.getNoOfClassVals(mapValues, classColName);
             featureInfo = MLlibNodeSettings.getNominalFeatureInfo(featureColNames, mapValues);
        } else {
            numberOfClasses = null;
            featureInfo = null;
        }

        return new MLlibSettings(tableSpec, classColName, classColIdx, numberOfClasses, featureColNames,
            featureColIdxs, featureInfo);
    }

    /**
     * @param model the PMML model
     * @return the field in the first FieldColumnPair of the MapValues mapped
     * to the MapValues Model
     */
    public static Map<String, DerivedField> getMapValues(final PMMLPortObject model) {
        final Map<String, DerivedField> mapValues = new LinkedHashMap<String, DerivedField>();
        final DerivedField[] derivedFields = model.getDerivedFields();
        for (final DerivedField derivedField : derivedFields) {
            final MapValues map = derivedField.getMapValues();
            if (null != map) {
                // This is the field name the mapValues is based on
                String name = derivedField.getDisplayName();
                if (name == null) {
                    name = derivedField.getName();
                }
                mapValues.put(name, derivedField);
            }
        }
        return mapValues;
    }

    /**
     * @param featureColNames the names of the feature columns in the same order as in the Spark RDD
     * @param mapValues the derived fields from the PMML
     * @return the nominal column indices as first element and the number of unique values of the corresponding
     * column as second argument
     */
    public static NominalFeatureInfo getNominalFeatureInfo(final List<String> featureColNames,
        final Map<String, DerivedField> mapValues) {
        final NominalFeatureInfo nominalFeatureInfo = new NominalFeatureInfo();
        int idx = 0;
        for (final String col : featureColNames) {
            final DerivedField derivedField = mapValues.get(col);
            if (derivedField != null) {
                final MapValues map = derivedField.getMapValues();
                final InlineTable table = map.getInlineTable();
                final int noOfVals = table.sizeOfRowArray();
                nominalFeatureInfo.add(Integer.valueOf(idx), noOfVals);
            }
            idx++;
        }
        return nominalFeatureInfo;
    }

    /**
     * @param mapValues the {@link DerivedField}
     * @param classColName the name of the class column
     * @return the number of unique classes if the class column is one of the {@link DerivedField}s
     */
    public static Long getNoOfClassVals(final Map<String, DerivedField> mapValues, final String classColName) {
        if (mapValues.containsKey(classColName)) {
            final DerivedField derivedField = mapValues.get(classColName);
            final MapValues map = derivedField.getMapValues();
            final InlineTable table = map.getInlineTable();
            final int noOfVals = table.sizeOfRowArray();
            return Long.valueOf(noOfVals);
        }
        return null;
    }

    /**
     * @param portIdx the index of the {@link SparkDataPortObjectSpec}
     * @param specs the {@link PortObjectSpec} array
     * @return the {@link DataTableSpec}[] that can be used in the loadSettingsFrom methods of the
     * {@link DialogComponent}s
     * @throws NotConfigurableException if the specs are not valid
     */
    public static DataTableSpec[] getTableSpecInDialog(final int portIdx, final PortObjectSpec[] specs) throws NotConfigurableException {
        if (specs == null || specs.length <= portIdx || specs[portIdx] == null) {
            throw new NotConfigurableException("No input connection available");
        }
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)specs[portIdx];
        final DataTableSpec[] tableSpecs = new DataTableSpec[] {spec.getTableSpec()};
        return tableSpecs;
    }

    /**
     * @return the {@link DialogComponent} that should be used for the feature column selection
     */
    public static DialogComponent createFeatureColsComponent() {
        return createFeatureColsComponent(0);
    }

    /**
     * @param portIdx the port index of the input {@link DataTableSpec}
     * @return the {@link DialogComponent} that should be used for the feature column selection
     */
    public static DialogComponent createFeatureColsComponent(final int portIdx) {
        return new DialogComponentColumnFilter2(createColumnsModel(), portIdx);
    }
    /**
     * @return the {@link DialogComponent} that should be used for the class column selection
     */
    public static DialogComponent createClassColComponent() {
        return createClassColComponent(0);
    }

    /**
     * @param portIdx the port index of the input {@link DataTableSpec}
     * @return the {@link DialogComponent} that should be used for the class column selection
     */
    @SuppressWarnings("unchecked")
    public static DialogComponent createClassColComponent(final int portIdx) {
        return new DialogComponentColumnNameSelection(createClassColModel(),
        "Class column ", portIdx, DoubleValue.class);
    }
}
