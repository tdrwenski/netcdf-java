package ucar.nc2.iosp.zarr;

import static com.google.common.truth.Truth.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.junit.Test;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;

public class TestZAttrs {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void shouldReadAttributes() throws IOException {
    String json = "{ \"numeric\" : 2, \"text\" : \"text_value\" }";
    ZAttrs zAttrs = readJson(json);
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
    Attribute attribute = zAttrs.getAttributes().get(0);
    assertThat(attribute.getDataType()).isEqualTo(DataType.INT);
    assertThat(attribute.getNumericValue()).isEqualTo(1);
  }

  @Test
  public void shouldReadDoubleAttribute() throws IOException {
    String json = "{ \"key\" : 1.1 }";
    ZAttrs zAttrs = readJson(json);
    Attribute attribute = zAttrs.getAttributes().get(0);
    assertThat(attribute.getDataType()).isEqualTo(DataType.DOUBLE);
    assertThat(attribute.getNumericValue()).isEqualTo(1.1);
  }

  @Test
  public void shouldReadStringAttribute() throws IOException {
    String json = "{ \"key\" : \"value\" }";
    ZAttrs zAttrs = readJson(json);
    Attribute attribute = zAttrs.getAttributes().get(0);
    assertThat(attribute.getDataType()).isEqualTo(DataType.STRING);
    assertThat(attribute.getStringValue()).isEqualTo("value");
  }

  @Test
  public void shouldReadArrayDimensions() throws IOException {
    String json = "{ \"_ARRAY_DIMENSIONS\" : [\"dim\"] }";
    ZAttrs zAttrs = readJson(json);
    assertThat(zAttrs.getAttributes().size()).isEqualTo(0);
    assertThat(zAttrs.getArrayDimensions().size()).isEqualTo(1);
    assertThat(zAttrs.getArrayDimensions().get(0)).isEqualTo("dim");
  }

  private ZAttrs readJson(String json) throws IOException {
    InputStream inputStream = new ByteArrayInputStream(json.getBytes());
    return objectMapper.readValue(inputStream, ZAttrs.class);
  }
}
