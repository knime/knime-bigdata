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
 *   Sep 25, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.type;

import static org.knime.filehandling.core.node.table.reader.type.hierarchy.TypeTester.createTypeTester;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import org.knime.core.data.DataType;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TreeTypeHierarchy;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TreeTypeHierarchy.TreeNode;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TreeTypeHierarchy.TreeTypeHierarchyBuilder;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TypeHierarchy;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TypeTester;

/**
 * Static class holding {@link TypeHierarchy TypeHierarchies} of {@link KnimeType KnimeTypes}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class KnimeTypeHierarchies {

    private static TreeTypeHierarchy<KnimeType, KnimeType>
        fromPrimitiveHierarchy(final TreeTypeHierarchy<KnimeType, KnimeType> hierarchy) {
        final TreeNode<KnimeType, KnimeType> root = hierarchy.getRootNode();
        // the root must accept all values
        final KnimeType rootType = root.getType();
        final TreeTypeHierarchyBuilder<KnimeType, KnimeType> builder =
            TreeTypeHierarchy.builder(createTypeTester(rootType, t -> true));
        final KnimeTypeBuilderFiller primitiveBuilderFiller =
            new KnimeTypeBuilderFiller(builder, UnaryOperator.identity(), KnimeTypeHierarchies::createPrimitivePredicate);
        primitiveBuilderFiller.addTypes(root, rootType);

        // the list subhierarchy is a mirror of the primitive hierarchy just for lists
        final ListKnimeType rootListType = new ListKnimeType(root.getType());
        builder.addType(rootType, createTypeTester(rootListType, KnimeType::isList));
        final KnimeTypeBuilderFiller listBuilderFiller =
            new KnimeTypeBuilderFiller(builder, ListKnimeType::new, KnimeTypeHierarchies::createListPredicate);
        listBuilderFiller.addTypes(root, rootListType);
        return builder.build();
    }

    private static Predicate<KnimeType> createPrimitivePredicate(final TypeTester<KnimeType, KnimeType> node) {
        return node::test;
    }

    private static Predicate<KnimeType> createListPredicate(final TypeTester<KnimeType, KnimeType> node) {
        return t -> t.isList() && node.test(t.asListType().getElementType());
    }

    /**
     * Helper class for filling a builder based on an existing hierarchy.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    private static final class KnimeTypeBuilderFiller {
        private final TreeTypeHierarchyBuilder<KnimeType, KnimeType> m_builder;

        private final Function<KnimeType, KnimeType> m_typeFactory;

        private final Function<TypeTester<KnimeType, KnimeType>, Predicate<KnimeType>> m_typePredicateFactory;

        KnimeTypeBuilderFiller(final TreeTypeHierarchyBuilder<KnimeType, KnimeType> builder,
            final UnaryOperator<KnimeType> typeFactory,
            final Function<TypeTester<KnimeType, KnimeType>, Predicate<KnimeType>> typePredicateFactory) {
            m_builder = builder;
            m_typePredicateFactory = typePredicateFactory;
            m_typeFactory = typeFactory;
        }

        void addTypes(final TreeNode<KnimeType, KnimeType> parentPrimitiveNode, final KnimeType parentType) {
            for (TreeNode<KnimeType, KnimeType> child : parentPrimitiveNode.getChildren()) {
                KnimeType childType = m_typeFactory.apply(child.getType());
                m_builder.addType(parentType, createTypeTester(childType, m_typePredicateFactory.apply(child)));
                addTypes(child, childType);
            }
        }

    }

    /**
     * The {@link TypeHierarchy} of primitive types.
     */
    public static final TreeTypeHierarchy<KnimeType, KnimeType> PRIMITIVE_TYPE_HIERARCHY = createTypeHierarchy();

    /**
     * The {@link TypeHierarchy} that includes flat list types in addition to the types contained in {@link #PRIMITIVE_TYPE_HIERARCHY}.
     */
    public static final TreeTypeHierarchy<KnimeType, KnimeType> TYPE_HIERARCHY =
        fromPrimitiveHierarchy(PRIMITIVE_TYPE_HIERARCHY);

    /**
     * The default type map for the types in {@link #TYPE_HIERARCHY}.
     */
    public static final Map<KnimeType, DataType> DEFAULT_TYPES =
        Collections.unmodifiableMap(createDefaultTypeMap(TYPE_HIERARCHY));

    private KnimeTypeHierarchies() {
        // static utility class
    }



    private static Map<KnimeType, DataType>
        createDefaultTypeMap(final TreeTypeHierarchy<KnimeType, KnimeType> typeHierarchy) {
        final Map<KnimeType, DataType> tm = new HashMap<>();
        fillDefaultTypeMap(tm, typeHierarchy.getRootNode());
        return tm;
    }

    private static void fillDefaultTypeMap(final Map<KnimeType, DataType> typeMap,
        final TreeNode<KnimeType, KnimeType> node) {
        final KnimeType type = node.getType();
        typeMap.put(type, type.getDefaultDataType());
        for (TreeNode<KnimeType, KnimeType> child : node.getChildren()) {
            fillDefaultTypeMap(typeMap, child);
        }
    }

    private static TreeTypeHierarchy<KnimeType, KnimeType> createTypeHierarchy() {
        final TreeTypeHierarchyBuilder<KnimeType, KnimeType> builder =
            TreeTypeHierarchy.builder(createTypeTester(PrimitiveKnimeType.STRING, t -> true));
        return builder.addType(PrimitiveKnimeType.STRING, createKnimeTypeTester(PrimitiveKnimeType.BOOLEAN))//
            .addType(PrimitiveKnimeType.STRING,
                createKnimeTypeTester(PrimitiveKnimeType.DOUBLE, PrimitiveKnimeType.INTEGER, PrimitiveKnimeType.LONG))//
            .addType(PrimitiveKnimeType.DOUBLE,
                createKnimeTypeTester(PrimitiveKnimeType.LONG, PrimitiveKnimeType.INTEGER))//
            .addType(PrimitiveKnimeType.LONG, createKnimeTypeTester(PrimitiveKnimeType.INTEGER)).build();

    }

    private static TypeTester<KnimeType, KnimeType> createKnimeTypeTester(final PrimitiveKnimeType type,
        final PrimitiveKnimeType... alternativeTypes) {
        final Predicate<PrimitiveKnimeType> typePredicate = isOneOf(type, alternativeTypes);
        return createTypeTester(type, t -> !t.isList() && typePredicate.test(t.asPrimitiveType()));
    }

    private static Predicate<PrimitiveKnimeType> isOneOf(final PrimitiveKnimeType type,
        final PrimitiveKnimeType... alternativeTypes) {
        Predicate<PrimitiveKnimeType> typePredicate = is(type);
        for (PrimitiveKnimeType alternativeType : alternativeTypes) {
            typePredicate = typePredicate.or(is(alternativeType));
        }
        return typePredicate;
    }

    private static Predicate<PrimitiveKnimeType> is(final PrimitiveKnimeType type) {
        return t -> t == type;
    }
}
