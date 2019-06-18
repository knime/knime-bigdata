/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
 *   Created on Jun 1, 2019 by bjoern
 */
package org.knime.bigdata.spark.core.port.model.ml;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLMetaData extends JobOutput {

    private static final String KEY_NOMINAL_FEATURE_VALUE_MAP = "nominalFeatureValueMap";

    private static final String KEY_NOMINAL_TARGET_VALUES = "nominalTargetValues";

    private static final String KEY_ADDITIONAL_VALUES = "additionalValues";

    public MLMetaData() {
        set(KEY_ADDITIONAL_VALUES, new HashMap<String, Object>());
    }

    public MLMetaData withNominalFeatureValueMapping(final int featureColIndex,
        final List<String> nominalValues) {
        Map<Integer, List<String>> nominalFeatureValueMap = get(KEY_NOMINAL_FEATURE_VALUE_MAP);
        if (nominalFeatureValueMap == null) {
            nominalFeatureValueMap = new HashMap<>();
            set(KEY_NOMINAL_FEATURE_VALUE_MAP, nominalFeatureValueMap);
        }
        nominalFeatureValueMap.put(featureColIndex, nominalValues);
        return this;
    }

    public MLMetaData withNominalTargetValueMappings(final List<String> nominalValues) {
        set(KEY_NOMINAL_TARGET_VALUES, nominalValues);
        return this;
    }

    public Map<Integer, List<String>> getNominalFeatureValueMappings() {
        Map<Integer, List<String>> toReturn = get(KEY_NOMINAL_FEATURE_VALUE_MAP);
        if (toReturn == null) {
            toReturn = Collections.emptyMap();
        }
        return toReturn;
    }

    public List<String> getNominalTargetValueMappings() {
        return get(KEY_NOMINAL_TARGET_VALUES);
    }

    public boolean hasNominalTargetValueMappings() {
        return has(KEY_NOMINAL_TARGET_VALUES);
    }

    public void setInteger(final String key, final Integer value) {
        getAdditionalValues().put(key, value);
    }

    @Override
    public Integer getInteger(final String key) {
        return (Integer) getAdditionalValues().get(key);
    }

    public void setDouble(final String key, final Double value) {
        getAdditionalValues().put(key, value);
    }

    public void setDoubleArray(final String key, final double[] values) {
        getAdditionalValues().put(key, values);
    }

    @Override
    public Double getDouble(final String key) {
        return (Double) getAdditionalValues().get(key);
    }

    public double[] getDoubleArray(final String key) {
        return (double[]) getAdditionalValues().get(key);
    }

    public void setString(final String key, final String value) {
        getAdditionalValues().put(key, value);
    }

    public String getString(final String key) {
        return (String) getAdditionalValues().get(key);
    }

    Map<String, Object> getAdditionalValues() {
        return get(KEY_ADDITIONAL_VALUES);
    }
}
