package be.vlaanderen.informatievlaanderen.ldes.ldi.processors.repository;

import static java.util.Map.entry;
import static java.util.Map.ofEntries;

import java.util.Map;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;

public class RdfDatatypeMapper {

  private static final Map<String, DataType> rdfMap =
      ofEntries(
          entry(XSDDatatype.XSDinteger.getURI(), RecordFieldType.LONG.getDataType()),
          entry(XSDDatatype.XSDint.getURI(), RecordFieldType.LONG.getDataType()),
          entry(XSDDatatype.XSDdecimal.getURI(), RecordFieldType.DOUBLE.getDataType()),
          entry(XSDDatatype.XSDfloat.getURI(), RecordFieldType.FLOAT.getDataType()),
          entry(XSDDatatype.XSDdouble.getURI(), RecordFieldType.DOUBLE.getDataType()),
          entry(XSDDatatype.XSDlong.getURI(), RecordFieldType.LONG.getDataType()),
          entry(XSDDatatype.XSDshort.getURI(), RecordFieldType.SHORT.getDataType()),
          entry(XSDDatatype.XSDbyte.getURI(), RecordFieldType.BYTE.getDataType()),
          entry(XSDDatatype.XSDboolean.getURI(), RecordFieldType.BOOLEAN.getDataType()),
          entry(XSDDatatype.XSDdate.getURI(), RecordFieldType.DATE.getDataType()),
          entry(XSDDatatype.XSDdateTime.getURI(), RecordFieldType.TIMESTAMP.getDataType()),
          entry(XSDDatatype.XSDtime.getURI(), RecordFieldType.TIME.getDataType()));
  // TODO: complete type mapping
  //        else if (t.getURI().equals(XSDDatatype.XSDunsignedByte.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDunsignedShort.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDunsignedInt.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDunsignedLong.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDnonPositiveInteger.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDnonNegativeInteger.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDpositiveInteger.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDnegativeInteger.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDhexBinary.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDbase64Binary.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDdateTimeStamp.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDduration.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDdayTimeDuration.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDyearMonthDuration.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDgDay.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDgMonth.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDgYear.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDgYearMonth.getURI())) {}
  //        else if (t.getURI().equals(XSDDatatype.XSDgMonthDay.getURI())) {}

  public static DataType getRecordType(String uri) {
    DataType dataType = rdfMap.get(uri);
    if(dataType == null) {
      return RecordFieldType.STRING.getDataType();
    }
    return dataType;
  }
}
