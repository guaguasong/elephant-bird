package com.twitter.elephantbird.hive.serde;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public final class ProtobufStructObjectInspector extends SettableStructObjectInspector {

  public static class ProtobufStructField implements StructField {

    private ObjectInspector oi = null;
    private String comment = null;
    private FieldDescriptor fieldDescriptor;
    private Message.Builder builder;

    @SuppressWarnings("unchecked")
    public ProtobufStructField(FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
      oi = this.createOIForField();
    }

    @SuppressWarnings("unchecked")
    public ProtobufStructField(FieldDescriptor fieldDescriptor, Message.Builder builder) {
      this.fieldDescriptor = fieldDescriptor;
      this.builder = builder;
      oi = this.createOIForField();
    }
    
    @Override
    public int getFieldID() {
      return fieldDescriptor.getIndex();
    }

    @Override
    public String getFieldName() {
      return fieldDescriptor.getName();
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return oi;
    }

    @Override
    public String getFieldComment() {
      return comment;
    }

    public FieldDescriptor getFieldDescriptor() {
      return fieldDescriptor;
    }

    private PrimitiveCategory getPrimitiveCategory(JavaType fieldType) {
      switch (fieldType) {
        case INT:
          return PrimitiveCategory.INT;
        case LONG:
          return PrimitiveCategory.LONG;
        case FLOAT:
          return PrimitiveCategory.FLOAT;
        case DOUBLE:
          return PrimitiveCategory.DOUBLE;
        case BOOLEAN:
          return PrimitiveCategory.BOOLEAN;
        case STRING:
          return PrimitiveCategory.STRING;
        case BYTE_STRING:
          return PrimitiveCategory.BINARY;
        case ENUM:
          return PrimitiveCategory.STRING;
        default:
          return null;
      }
    }

    private ObjectInspector createOIForField() {
      JavaType fieldType = fieldDescriptor.getJavaType();
      PrimitiveCategory category = getPrimitiveCategory(fieldType);
      ObjectInspector elementOI = null;
      if (category != null) {
        elementOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(category);
      } else {
        switch (fieldType) {
          case MESSAGE:
            elementOI = new ProtobufStructObjectInspector(
                fieldDescriptor.getMessageType(),
                builder.newBuilderForField(fieldDescriptor));
            break;
          default:
            throw new RuntimeException("JavaType " + fieldType
                + " from protobuf is not supported.");
        }
      }
      if (fieldDescriptor.isRepeated()) {
        return ObjectInspectorFactory.getStandardListObjectInspector(elementOI);
      } else {
        return elementOI;
      }
    }
  }

  private Descriptor descriptor;
  private Message.Builder builder;
  private List<StructField> structFields = Lists.newArrayList();

  ProtobufStructObjectInspector(Descriptor descriptor, Message.Builder builder) {
    this.descriptor = descriptor;
    this.builder = builder;
    for (FieldDescriptor fd : descriptor.getFields()) {
      if (fd.getJavaType() == JavaType.MESSAGE) {
        structFields.add(new ProtobufStructField(fd, builder));
      } else {
        structFields.add(new ProtobufStructField(fd));
      }
    }
  }
  
  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public String getTypeName() {
    StringBuilder sb = new StringBuilder("struct<");
    boolean first = true;
    for (StructField structField : getAllStructFieldRefs()) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(structField.getFieldName()).append(":")
          .append(structField.getFieldObjectInspector().getTypeName());
    }
    sb.append(">");
    return sb.toString();
  }

  @Override
  public Object create() {
    return builder.build().newBuilderForType();
  }

  @Override
  public Object setStructFieldData(Object data, StructField field, Object fieldValue) {
    Message.Builder builder = (Message.Builder)data;
    FieldDescriptor fieldDescriptor = descriptor.findFieldByName(field.getFieldName());

    builder.clearField(fieldDescriptor);

    if (fieldDescriptor.isRepeated()) {
      for (Object b : ((ArrayList<?>)fieldValue)) {
        if (fieldDescriptor.getType() == Type.MESSAGE) {
          builder.addRepeatedField(fieldDescriptor, ((Message.Builder)b).build());
        } else {
          builder.addRepeatedField(fieldDescriptor, b);
        }
      }
    } else {
      if (fieldDescriptor.getType() == Type.MESSAGE) {
        builder.setField(fieldDescriptor, ((Message.Builder)fieldValue).build());
      } else {
        builder.setField(fieldDescriptor, fieldValue);
      }
    }
    return builder;
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return structFields;
  }

  @Override
  public Object getStructFieldData(Object data, StructField structField) {
    if (data == null) {
      return null;
    }
    MessageOrBuilder m = (MessageOrBuilder) data;
    ProtobufStructField psf = (ProtobufStructField) structField;
    FieldDescriptor fieldDescriptor = psf.getFieldDescriptor();
    Object result = m.getField(fieldDescriptor);
    if (fieldDescriptor.getType() == Type.ENUM) {
      return ((EnumValueDescriptor)result).getName();
    }
    if (fieldDescriptor.getType() == Type.BYTES && (result instanceof ByteString)) {
      return ((ByteString)result).toByteArray();
    }
    if (fieldDescriptor.getType() == Type.MESSAGE && !fieldDescriptor.isRepeated()) {
      result = ((Message)result).toBuilder();
    }
    return result;
  }

  @Override
  public StructField getStructFieldRef(String fieldName) {
    FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
    if (fieldDescriptor.getJavaType() == JavaType.MESSAGE) {
      return new ProtobufStructField(fieldDescriptor, builder);
    }
    return new ProtobufStructField(fieldDescriptor);
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    List<Object> result = Lists.newArrayList();
    Message m = (Message) data;
    for (FieldDescriptor fd : descriptor.getFields()) {
      result.add(m.getField(fd));
    }
    return result;
  }
}
