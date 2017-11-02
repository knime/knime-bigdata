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
 *   Created on Apr 14, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.model;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public interface LegacyModelHelper extends ModelHelper {

    public final static String LEGACY_MODEL_NAME = "legacy";

    /**
     * Inspects the given model instance and tries to guess the unique model name under which a real {@link ModelHelper}
     * is available. This is used when loading old workflows that did not save the model name, just the model instance.
     *
     * @param modelInstance
     * @return the guessed model name, or null, if no plausible model name could be determined.
     */
    String tryToGuessModelName(final Object modelInstance);

    /**
     * Returns a "magic" object input stream that has all the required classes in its classpath that are required to
     * load legacy workflows (classes from the Spark API, classes that were removed/changed in KNIME).
     *
     * @param in An input stream
     * @return an {@link ObjectInputStream} with a special classpath that allows to load legacy workflows
     * @throws IOException if something went wrong while creating the stream
     */
    ObjectInputStream getObjectInputStream(InputStream in) throws IOException;

    /**
     * Inspects the given model instance and, if necessary, converts it into the current model. If the given model
     * instance does not need to be converted, this method does nothing.
     * 
     * Converting legacy models may be necessary when loading KNIME workflows with Spark nodes, that were saved before
     * KNIME Spark Executor 1.6.0 (an example would be the collaborative filtering models).
     */
    Object convertLegacyToNewModel(final Object modelInstance);
}
