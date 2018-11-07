package org.knime.bigdata.fileformats.parquet.datatype.mapping;

import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

/**
 * ParquetType for Parquet type mapping
 * 
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 */
public class ParquetType {

    private OriginalType m_original;
    private PrimitiveTypeName m_primitive;
    private String m_name;
    private ParquetType m_elementType;

    /**
     * Constructs a ParquetType
     * 
     * @param original the original Parquet type 
     * @param primitive the primitive Parquet type
     */
    public ParquetType(PrimitiveTypeName primitive, OriginalType original) {
        m_original = original;
        m_primitive = primitive;
    }

    /**
     * Constructs a ParquetType
     * 
     * @param primitive the primitive Parquet type
     */
    public ParquetType(PrimitiveTypeName primitive) {
        this(primitive, null);
    }

    /**
     * Constructs a ParquetType for lists
     */
    private ParquetType(ParquetType element) {
        m_original = OriginalType.LIST;
        m_elementType = element;
    }

    /**
     * @return the m_original
     */
    protected OriginalType getOriginal() {
        return m_original;
    }

    /**
     * @return the m_primitive
     */
    protected PrimitiveTypeName getPrimitive() {
        return m_primitive;
    }

    @Override
    public String toString() {
        if (m_original == OriginalType.LIST) {
            return String.format("LIST[%s]", m_elementType.toString());
        }

        if (m_original != null) {
            return String.format("%s (%s)", m_original, m_primitive);
        }
        return String.format("%s", m_primitive);
    }

    /**
     * Creates a list Parquet type.
     * 
     * @param elementType the type of the elements
     * @return a ParquetType for lists
     */
    public static ParquetType createListType(ParquetType elementType) {
        return new ParquetType(elementType);
    }

    /**
     * Creates a ParquetType from the given String.
     * 
     * @param string
     *            the string to parse
     * @return the resulting ParquetType
     */
    public static ParquetType fromString(String string) {
        OriginalType original = null;
        PrimitiveTypeName primitive = null;
        if (string.startsWith("LIST")) {
            String subtype = string.substring(string.indexOf('[') + 1, string.lastIndexOf(']'));
            ParquetType element = ParquetType.fromString(subtype);
            return new ParquetType(element);

        } else if (string.contains("(")) {
            String org = string.substring(0, string.indexOf(' '));
            original = OriginalType.valueOf(org);
            String prim = string.substring(string.indexOf('(') + 1, string.lastIndexOf(')'));
            primitive = PrimitiveTypeName.valueOf(prim);
        }
        return new ParquetType(primitive, original);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if ((obj.getClass() != this.getClass())) {
            return false;
        }
        ParquetType other = (ParquetType) obj;
        boolean elementEqual = m_elementType == null ? other.m_elementType == null
                : m_elementType.equals(other.m_elementType);

        return m_original == other.m_original && m_primitive == other.m_primitive && elementEqual;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_original == null) ? 0 : m_original.hashCode());
        result = prime * result + ((m_primitive == null) ? 0 : m_primitive.hashCode());
        result = prime * result + ((m_elementType == null) ? 0 : m_elementType.hashCode());
        result = prime * result + ((m_name == null) ? 0 : m_name.hashCode());
        return result;
    }

    /**
     * @return the name
     */
    public String getName() {
        return m_name;
    }

    /**
     * Constructs a Parquet Type object with the given Name
     * 
     * @param name
     *            the name for the Type
     * @return the Type object
     */
    public Type constructParquetType(String name) {
        m_name = name;
        if (m_original == OriginalType.LIST) {
            Type element = m_elementType.constructParquetType("element");
            return Types.optionalList().element(element).named(name);
        } else {
            return new PrimitiveType(Repetition.OPTIONAL, m_primitive, name, m_original);
        }
    }

}
