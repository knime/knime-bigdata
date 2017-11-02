/*
 * ------------------------------------------------------------------ This source code, its documentation and all
 * appendant files are protected by copyright law. All rights reserved. Copyright by KNIME AG, Zurich, Switzerland You
 * may not modify, publish, transmit, transfer or sell, reproduce, create derivative works from, distribute, perform,
 * display, or in any way exploit any of the content, in whole or in part, except as otherwise expressly permitted in
 * writing by the copyright owner or as specified in the license file distributed with this product. If you have any
 * questions please contact the copyright holder: website: www.knime.com email: contact@knime.com
 * --------------------------------------------------------------------- History Created on 04.06.2015 by dwk
 */
package com.knime.bigdata.spark1_2.api;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.node.preproc.convert.NominalValueMapping;


/**
 *
 * @author dwk
 */
@SparkClass
public class LabeledDataInfo {
  private final JavaRDD<LabeledPoint> m_labeledPointRDD;
  private final NominalValueMapping m_labelToIntMapping;

  /**
   * stores references to given args, no copies are made
   *
   * @param aLabeledPointRDD
   * @param aLabelToIntMapping mapping of nominal values to ints
   */
  public LabeledDataInfo(final JavaRDD<LabeledPoint> aLabeledPointRDD,
      final NominalValueMapping aLabelToIntMapping) {
    m_labeledPointRDD = aLabeledPointRDD;
    m_labelToIntMapping = aLabelToIntMapping;
  }

  /**
   * @return RDD with labeled points
   */
  public JavaRDD<LabeledPoint> getLabeledPointRDD() {
    return m_labeledPointRDD;
  }

  /**
   * @return Vector part of the labeled point RDD
   */
  public JavaRDD<Vector> getVectorRDD() {
	  return RDDUtils.toVectorRDDFromLabeledPointRDD(m_labeledPointRDD);
  }

  /**
   * @return mapping of String (class) label to integer class
   */
  public NominalValueMapping getClassLabelToIntMapping() {
    return m_labelToIntMapping;
  }
}
