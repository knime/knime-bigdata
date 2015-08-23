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
 *   Created on 21.07.2015 by koetter
 */
package com.knime.bigdata.spark.port.model.interpreter;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextPane;

import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.SparkModelPortObjectSpec;

/**
 * HTML panel that describes the given {@link SparkModel}.
 * @author Tobias Koetter, KNIME.com
 */
class HTMLModelView extends JPanel {

    private static final long serialVersionUID = 1L;

    /**
     * @param model the serialised {@link SparkModelPortObjectSpec}
     * @param htmlDescription the html description of the model
     */
    HTMLModelView(final SparkModel<?> model, final String htmlDescription) {
        super(new GridBagLayout());
        super.setName("Model Details");
        StringBuilder buf = new StringBuilder("<html><body>");
        buf.append("<strong>Type:</strong>&nbsp;&nbsp;");
        buf.append("<tt>" + model.getType() + "</tt>");
//        buf.append("<strong>Learning columns:</strong>&nbsp;&nbsp;");
//        buf.append(model.getLearningColumnNames());
        if (model.getClassColumnName() != null) {
            buf.append("&nbsp;&nbsp;<strong>Class Column:</strong>&nbsp;&nbsp;");
            buf.append(model.getClassColumnName());
        }
        buf.append("<br/><hr>");
//        buf.append("<strong>Details:</strong>&nbsp;&nbsp;<br><hr>");
        buf.append(htmlDescription);
        buf.append("<br/>");
        buf.append("</body></html>");
        final JTextPane textArea = new JTextPane();
        textArea.setContentType("text/html");
        textArea.setEditable(false);
        textArea.setText(buf.toString());
        textArea.setCaretPosition(0);
        final JScrollPane jsp = new JScrollPane(textArea);
        jsp.setPreferredSize(new Dimension(300, 300));
        final GridBagConstraints c = new GridBagConstraints();
        c.gridx = 0;
        c.gridy = 0;
        c.anchor = GridBagConstraints.CENTER;
        c.fill = GridBagConstraints.BOTH;
        c.weightx = 1;
        c.weighty = 1;
        super.add(jsp, c);
    }
}
