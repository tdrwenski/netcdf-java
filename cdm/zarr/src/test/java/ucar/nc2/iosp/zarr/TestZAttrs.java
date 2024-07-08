package ucar.nc2.iosp.zarr;

import static com.google.common.truth.Truth.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.junit.Test;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;

public class TestZAttrs {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void shouldReadAttributes() throws IOException {
    String json = "{ \"numeric\" : 2, \"text\" : \"text_value\" }";
    ZAttrs zAttrs = readJson(json);
    assertThat(zAttrs.getArrayDimensions().size()).isEqualTo(0);
    List<Attribute> attributes = zAttrs.getAttributes();
    assertThat(attributes.size()).isEqualTo(2);
    assertThat(attributes.get(0).getDataType()).isEqualTo(DataType.INT);
    assertThat(attributes.get(0).getName()).isEqualTo("numeric");
    assertThat(attributes.get(0).getNumericValue()).isEqualTo(2);
    assertThat(attributes.get(1).getDataType()).isEqualTo(DataType.STRING);
    assertThat(attributes.get(1).getName()).isEqualTo("text");
    assertThat(attributes.get(1).getStringValue()).isEqualTo("text_value");
  }

  @Test
  public void shouldReadIntAttribute() throws IOException {
    String json = "{ \"key\" : 1 }";
    ZAttrs zAttrs = readJson(json);
    checkAttribute(zAttrs.getAttributes(), DataType.INT, 1);
  }

  @Test
  public void shouldReadDoubleAttribute() throws IOException {
    String json = "{ \"key\" : 1.1 }";
    ZAttrs zAttrs = readJson(json);
    checkAttribute(zAttrs.getAttributes(), DataType.DOUBLE, 1.1);
  }

  @Test
  public void shouldReadStringAttribute() throws IOException {
    String json = "{ \"key\" : \"value\" }";
    ZAttrs zAttrs = readJson(json);
    checkAttribute(zAttrs.getAttributes(), DataType.STRING, "value");
  }

  @Test
  public void shouldReadTypedAttribute() throws IOException {
//    checkTypedAttribute(true, "<b1", true, DataType.BOOLEAN);
//    checkTypedAttribute(5.1, "<i1", 5, DataType.BYTE);
//    checkTypedAttribute(5.1, "<u1", 5, DataType.UBYTE);
//    checkTypedAttribute(5.1, "<i2", 5, DataType.SHORT);
//    checkTypedAttribute(5.1, "<u2", 5, DataType.USHORT);
//    checkTypedAttribute(5.1, "<i4", 5, DataType.INT);
//    checkTypedAttribute(5.1, "<u4", 5, DataType.UINT);
//    checkTypedAttribute(5.1, "<i8", 5, DataType.LONG);
//    checkTypedAttribute(5.1, "<u8", 5, DataType.ULONG);
//    checkTypedAttribute(5.1, "<f4", 5.1f, DataType.FLOAT);
//    checkTypedAttribute(5.1, "<f8", 5.1, DataType.DOUBLE);
    // checkTypedAttribute("\"foo\"", "<O1", "foo", DataType.OBJECT); // TODO is this a json object?
//    checkTypedAttribute("\"foo\"", "<S4", "foo", DataType.STRING);
//    checkTypedAttribute("\"foo\"", "<U4", "foo", DataType.STRING);
     checkTypedAttribute("\"f\"", "<S1", 'f', DataType.CHAR); // TODO chars? Attributes with type char get convert to string
    // checkTypedAttribute("\"f\"", "<U1", 'f', DataType.CHAR); // TODO chars?
  }

  @Test
  public void shouldReadArrayDimensions() throws IOException {
    String json = "{ \"_ARRAY_DIMENSIONS\" : [\"dim1\", \"dim2\"] }";
    ZAttrs zAttrs = readJson(json);
    assertThat(zAttrs.getAttributes().size()).isEqualTo(0);
    assertThat(zAttrs.getArrayDimensions().size()).isEqualTo(2);
    assertThat(zAttrs.getArrayDimensions().get(0)).isEqualTo("dim1");
    assertThat(zAttrs.getArrayDimensions().get(1)).isEqualTo("dim2");
  }

  private void checkTypedAttribute(Object value, String type, Object expectedValue, DataType expectedDataType)
      throws IOException {
    String json = "{ \"key\" : " + value + ", \"_nczarr_attr\": {\"types\": {\"key\": \"" + type + "\"} } }";
    ZAttrs zAttrs = readJson(json);
    checkAttribute(zAttrs.getAttributes(), expectedDataType, expectedValue);
  }

  private void checkAttribute(List<Attribute> attributes, DataType expectedType, Object expectedValue) {
    assertThat(attributes.size()).isEqualTo(1);
    Attribute attribute = attributes.get(0);
    assertThat(attribute.getDataType()).isEqualTo(expectedType);

    Array array = attribute.getValues();
    assertThat(array).isNotNull();
    assertThat(array.getDataType()).isEqualTo(expectedType);
    assertThat(array.getSize()).isEqualTo(1);
    assertThat(array.getObject(0)).isEqualTo(expectedValue);
  }

  private ZAttrs readJson(String json) throws IOException {
    InputStream inputStream = new ByteArrayInputStream(json.getBytes());
    return objectMapper.readValue(inputStream, ZAttrs.class);
  }
}
