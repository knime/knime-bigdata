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
 *   Created on 22.05.2015 by dwk
 */
package org.knime.bigdata.spark.core.sparkjobserver.rest;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

/**
 *
 * @author dwk, Bjoern Lohrmann, KNIME.COM
 */
interface IRestClient {

    <T> Response post(final String aPath, final String[] aArgs, final Entity<T> aEntity);

    Response delete(final String aPath);

    Response get(final String aPath);

}