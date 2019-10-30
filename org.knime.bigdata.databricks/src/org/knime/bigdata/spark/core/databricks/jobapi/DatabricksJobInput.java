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
 *   Created on Apr 11, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.databricks.jobapi;

import java.nio.file.Path;

import org.knime.bigdata.spark.core.job.JobData;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * A class that wraps that wraps the actual job input for Spark jobs on Databricks.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class DatabricksJobInput extends JobData {

    private static final String DATABRICKS_PREFIX = "databricks";

    private static final String KEY_JOBINPUT_CLASS = "jobInputClass";

    private static final String KEY_JOB_CLASS = "jobClass";

    /**
     * Empty constructor for (de)serialization.
     */
    public DatabricksJobInput() {
        super(DATABRICKS_PREFIX);
    }

    DatabricksJobInput(final JobInput jobInput, final String sparkJobClass) {
        super(DATABRICKS_PREFIX, jobInput.getInternalMap());
        set(KEY_JOBINPUT_CLASS, jobInput.getClass().getCanonicalName());
        set(KEY_JOB_CLASS, sparkJobClass);
        for (Path inputFile : jobInput.getFiles()) {
            withFile(inputFile);
        }
    }

    /**
     * @return the Spark job class name
     */
    public String getSparkJobClass() {
        return get(KEY_JOB_CLASS);
    }

    /**
     * Instantiates and returns the wrapped {@link JobInput} backed by the same internal map.
     *
     * @return The wrapped spark job input
     *
     * @throws ClassNotFoundException If something went wrong during instantiation.
     * @throws InstantiationException If something went wrong during instantiation.
     * @throws IllegalAccessException If something went wrong during instantiation.
     */
    @SuppressWarnings("unchecked")
    public <T extends JobInput> T getSparkJobInput()
        throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        final String jobInputClassName = get(KEY_JOBINPUT_CLASS);
        final T jobInput = (T)getClass().getClassLoader().loadClass(jobInputClassName).newInstance();

        jobInput.setInternalMap(getInternalMap());
        for (Path inputFile : getFiles()) {
            jobInput.withFile(inputFile);
        }

        return jobInput;
    }

    /**
     * Wraps the given job input into an instance of {@link DatabricksJobInput}.
     *
     * @param jobInput the {@link JobInput}
     * @param sparkJobClass the Spark job class to use
     * @return the {@link DatabricksJobInput}
     */
    public static DatabricksJobInput createFromSparkJobInput(final JobInput jobInput,
        final String sparkJobClass) {
        return new DatabricksJobInput(jobInput, sparkJobClass);
    }
}
