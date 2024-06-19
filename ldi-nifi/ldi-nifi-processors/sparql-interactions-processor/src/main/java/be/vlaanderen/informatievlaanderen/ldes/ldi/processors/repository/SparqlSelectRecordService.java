package be.vlaanderen.informatievlaanderen.ldes.ldi.processors.repository;

import org.apache.commons.lang3.tuple.Pair;
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
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class SparqlSelectRecordService {

  public static Pair<RecordSchema, List<Record>> getRecordsWithSchema(
      byte[] incomingRecord, Lang rdfLang, String query) {
    final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(incomingRecord);
    Model model = RDFParser.source(byteArrayInputStream).lang(rdfLang).build().toModel();

    return executeSelect(model, query, SparqlSelectRecordService::getSchemaRecordsListPair);
  }

  public static FlowFile writeRecords(
      ProcessSession session,
      Pair<RecordSchema, List<Record>> firstRecords,
      RecordSetWriterFactory writerFactory,
      FlowFile original,
      ComponentLog logger)
      throws IOException, SchemaNotFoundException, MalformedRecordException {
    return writeRecords(session, firstRecords, writerFactory, original, logger, null, null);
  }

  public static FlowFile writeRecords(
      ProcessSession session,
      Pair<RecordSchema, List<Record>> firstRecords,
      RecordSetWriterFactory writerFactory,
      FlowFile original,
      ComponentLog logger,
      RecordReader reader,
      Function<Record, Pair<?, List<Record>>> queryRecordProvider)
      throws IOException, SchemaNotFoundException, MalformedRecordException {

    FlowFile resultingFlowFile = session.create(original);
    RecordSchema writeSchema =
        writerFactory.getSchema(original.getAttributes(), firstRecords.getLeft());
    Map<String, String> attributes = new HashMap<>();
    try (final OutputStream out = session.write(resultingFlowFile);
        final RecordSetWriter writer =
            getWriter(writerFactory, writeSchema, out, resultingFlowFile, logger)) {

      writer.beginRecordSet();

      for (Record queryResult : firstRecords.getRight()) {
        writer.write(queryResult);
      }
      if (reader != null && queryRecordProvider != null) {
        Record incomingRecord;
        while ((incomingRecord = reader.nextRecord()) != null) {

          for (Record queryResult : queryRecordProvider.apply(incomingRecord).getRight()) {
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

  public static Pair<RecordSchema, List<Record>> getSchemaRecordsListPair(ResultSet resultSet) {
    RecordSchema recordSchema = deriveSchema(resultSet);
    List<Record> records = toRecords(resultSet, recordSchema);
    return Pair.of(recordSchema, records);
  }

  public static <R> R executeSelect(
      Model inputModel, String queryString, Function<ResultSet, R> transformResultSet) {
    final Query query = QueryFactory.create(queryString);
    try (QueryExecution queryExecution = QueryExecutionFactory.create(query, inputModel)) {
      ResultSet resultSet = queryExecution.execSelect();
      return transformResultSet.apply(resultSet);
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
      valueMap.put(varName, literal.getValue());
    }
    return new MapRecord(recordSchema, valueMap);
  }

  private static RecordSchema deriveSchema(ResultSet resultSet) {
    List<RecordField> recordFields = new ArrayList<>();
    for (String varName : resultSet.getResultVars()) {
      recordFields.add(new RecordField(varName, RecordFieldType.STRING.getDataType()));
    }
    return new SimpleRecordSchema(recordFields);
  }
}
