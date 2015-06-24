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
 *   Created on 19.06.2015 by koetter
 */
package com.knime.bigdata.spark.node.scripting.util;

import java.awt.Color;

import org.fife.ui.rsyntaxtextarea.Token;
import org.fife.ui.rsyntaxtextarea.folding.FoldParserManager;
import org.knime.base.node.jsnippet.guarded.GuardedDocument;
import org.knime.base.node.jsnippet.guarded.GuardedSection;
import org.knime.base.node.jsnippet.guarded.GuardedSectionsFoldParser;
import org.knime.base.node.util.KnimeSyntaxTextArea;


/**
 *
 * @author koetter
 */
public class SparkJavaSnippetTextArea extends KnimeSyntaxTextArea {
    private static final long serialVersionUID = 1L;

    /**
     * Create a new component.
     * @param snippet the snippet
     */
    public SparkJavaSnippetTextArea(final SparkJavaSnippet snippet) {
        // initial text != null causes a null pointer exception
        super(new SparkJavaSnippetDocument(""), null, 20, 60);

        setDocument(snippet.getDocument());
        addParser(snippet.getParser());

        boolean parserInstalled = FoldParserManager.get().getFoldParser(
                SYNTAX_STYLE_JAVA) instanceof GuardedSectionsFoldParser;
        if (!parserInstalled) {
            FoldParserManager.get().addFoldParserMapping(SYNTAX_STYLE_JAVA,
                    new GuardedSectionsFoldParser());
        }
        setCodeFoldingEnabled(true);
        setSyntaxEditingStyle(SYNTAX_STYLE_JAVA);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Color getForegroundForToken(final Token t) {
        if (isInGuardedSection(t.getOffset())) {
            return Color.gray;
        } else {
            return super.getForegroundForToken(t);
        }
    }

    /**
     * Returns true when offset is within a guarded section.
     *
     * @param offset the offset to test
     * @return true when offset is within a guarded section.
     */
    private boolean isInGuardedSection(final int offset) {
        GuardedDocument doc = (GuardedDocument)getDocument();

        for (String name : doc.getGuardedSections()) {
            GuardedSection gs = doc.getGuardedSection(name);
            if (gs.contains(offset)) {
                return true;
            }
        }
        return false;
    }
}
