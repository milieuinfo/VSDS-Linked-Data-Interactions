package be.vlaanderen.informatievlaanderen.ldes.ldi.processors.repository;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

public class SparqlSelectRecordService {

  public static ResultSetRewindable getQueryResults(byte[] incomingRecord, Lang rdfLang, String query) {
    final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(incomingRecord);
    Model model = RDFParser.source(byteArrayInputStream).lang(rdfLang).build().toModel();
    return executeSelect(model, query);
  }

  public static FlowFile writeRecords(
      ProcessSession session,
      List<Record> firstRecords,
      RecordSchema schema,
      RecordSetWriterFactory writerFactory,
      FlowFile original,
      ComponentLog logger)
      throws IOException, SchemaNotFoundException, MalformedRecordException {
    return writeRecords(session, firstRecords, schema, writerFactory, original, logger, null, null);
  }

  public static FlowFile writeRecords(
      ProcessSession session,
      List<Record> firstRecords,
      RecordSchema writeSchema,
      RecordSetWriterFactory writerFactory,
      FlowFile original,
      ComponentLog logger,
      RecordReader reader,
      Function<Record, List<Record>> queryRecordProvider)
      throws IOException, SchemaNotFoundException, MalformedRecordException {

    FlowFile resultingFlowFile = session.create(original);

    Map<String, String> attributes = new HashMap<>();
    try (final OutputStream out = session.write(resultingFlowFile);
        final RecordSetWriter writer =
            getWriter(writerFactory, writeSchema, out, resultingFlowFile, logger)) {

      writer.beginRecordSet();

      for (Record queryResult : firstRecords) {
        writer.write(queryResult);
      }
      if (reader != null && queryRecordProvider != null) {
        Record incomingRecord;
        while ((incomingRecord = reader.nextRecord()) != null) {

          for (Record queryResult : queryRecordProvider.apply(incomingRecord)) {
            writer.write(queryResult);
          }
        }
      }

      WriteResult writeResult = writer.finishRecordSet();

      attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
      attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
      attributes.putAll(writeResult.getAttributes());
    } catch (Exception e) {
      if (resultingFlowFile != null) {
        session.remove(resultingFlowFile);
      }
      throw e;
    }
    resultingFlowFile = session.putAllAttributes(resultingFlowFile, attributes);
    return resultingFlowFile;
  }

  public static RecordSetWriter getWriter(
      RecordSetWriterFactory writerFactory,
      RecordSchema writeSchema,
      OutputStream out,
      FlowFile transformed,
      ComponentLog logger)
      throws SchemaNotFoundException, IOException {
    return writerFactory.createWriter(logger, writeSchema, out, transformed);
  }

  public static List<Record> getSchemaRecordsListPair(ResultSet resultSet, RecordSchema schema) {
    List<Record> records = toRecords(resultSet, schema);
    return records;
  }

  public static ResultSetRewindable executeSelect(Model inputModel, String queryString) {
    final Query query = QueryFactory.create(queryString);
    try (QueryExecution queryExecution = QueryExecutionFactory.create(query, inputModel)) {
      return (ResultSetRewindable) queryExecution.execSelect().materialise();
    }
  }

  private static List<Record> toRecords(ResultSet resultSet, RecordSchema recordSchema) {
    List<Record> records = new ArrayList<>();
    List<String> resultVars = resultSet.getResultVars();
    while (resultSet.hasNext()) {
      QuerySolution querySolution = resultSet.next();
      records.add(toRecord(querySolution, resultVars, recordSchema));
    }
    return records;
  }

  private static Record toRecord(
      QuerySolution querySolution, List<String> resultVars, RecordSchema recordSchema) {
    Map<String, Object> valueMap = new HashMap<>();
    for (String varName : resultVars) {
      RDFNode node = querySolution.get(varName);
      if (node == null) {
        valueMap.put(varName, null);
        continue;
      }

      if (node.isResource()) {
        Resource resourceNode = node.asResource();
        String value =
            resourceNode.isURIResource()
                ? resourceNode.getURI()
                : resourceNode.isAnon()
                    ? resourceNode.getId().getLabelString()
                    : resourceNode.toString();
        valueMap.put(varName, value);
        continue;
      }

      if (!node.isLiteral()) {
        valueMap.put(varName, node.toString());
        continue;
      }

      Literal literal = node.asLiteral();

      valueMap.put(varName, getValue(recordSchema, varName, literal));
    }
    return new MapRecord(recordSchema, valueMap);
  }

  private static Object getValue(RecordSchema recordSchema, String varName, Literal literal) {
    Optional<DataType> dataType = recordSchema.getDataType(varName);
    if (dataType.isPresent() && dataType.get().equals(RecordFieldType.STRING.getDataType())) {
      return literal.getLexicalForm();
    }

    if (dataType.isPresent() && (dataType.get().equals(RecordFieldType.DATE.getDataType()) || dataType.get().equals(RecordFieldType.TIMESTAMP.getDataType()))) {
      Calendar calendar = ((XSDDateTime) literal.getValue()).asCalendar();
      return calendar.getTimeInMillis();
    }

//    if (dataType.isPresent() && dataType.get().equals(RecordFieldType.DECIMAL.getDataType())) {
//      return literal.getDouble();
//    }

    return literal.getValue();
  }

  public static RecordSchema deriveSchema(ResultSetRewindable resultSet) {
    List<RecordField> fields = new ArrayList<>();
    while (resultSet.hasNext()) {
      QuerySolution qs = resultSet.next();
      Iterator<String> it = qs.varNames();
      while (it.hasNext()) {
        String column = it.next();
        RDFNode n = qs.get(column);

        // STRING as default data type
        DataType dt = RecordFieldType.STRING.getDataType();
        if (n.isLiteral()) {
          dt = RdfDatatypeMapper.getRecordType(n.asLiteral().getDatatype().getURI());
        }
        if (fields.stream().noneMatch(rf -> rf.getFieldName().equals(column))) {
          fields.add(new RecordField(column, dt));
        }
      }
    }
    resultSet.reset();
    return new SimpleRecordSchema(fields);
  }
}
