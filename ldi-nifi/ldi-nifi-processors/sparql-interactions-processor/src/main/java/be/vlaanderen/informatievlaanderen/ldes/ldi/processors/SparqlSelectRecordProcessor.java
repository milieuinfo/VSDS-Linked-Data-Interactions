package be.vlaanderen.informatievlaanderen.ldes.ldi.processors;

import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.DATA_SOURCE_FORMAT;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.RDF_PAYLOAD_FIELD;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.RECORD_READER;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.RECORD_WRITER;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.SPARQL_SELECT_QUERY;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.repository.SparqlSelectRecordService.deriveSchema;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.repository.SparqlSelectRecordService.getQueryResults;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.repository.SparqlSelectRecordService.getSchemaRecordsListPair;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.repository.SparqlSelectRecordService.writeRecords;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.services.FlowManager.FAILURE;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.services.FlowManager.SUCCESS;

import be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetRewindable;
import org.apache.jena.riot.Lang;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StopWatch;

/**
 * // * TODO Extra relation voor empty output // * TODO Configure custom recordschema and implement
 * mapping from sparql query solutions to records // * TODO Refactoring
 */
@Tags({"ldes, rdf, SPARQL, record, select"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
  @WritesAttribute(
      attribute = "mime.type",
      description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
  @WritesAttribute(
      attribute = "record.count",
      description = "The number of records in the FlowFile"),
  @WritesAttribute(
      attribute = "error.message",
      description = "This attribute provides on failure the error message.")
})
@CapabilityDescription(
    "Reads a FlowFile's RDF data content, executes the provided SPARQL select query on the parsed RDF data and writes the resultset to records in a new flowFile. "
        + "If the incoming flowfile contains records of RDF data.a record reader must be configured and the record field containing the RDF payload.")
public class SparqlSelectRecordProcessor extends AbstractProcessor {

  static final Relationship REL_ORIGINAL =
      new Relationship.Builder()
          .name("original")
          .description(
              "The original FlowFile that was transformed. If the FlowFile fails processing, nothing will be sent to this relationship")
          .build();

  static final Relationship REL_EMPTY =
      new Relationship.Builder()
          .name("empty")
          .description(
              "The original FlowFile that was transformed. If the query is empty, nothing will be sent to this relationship")
          .build();

  @Override
  public Set<Relationship> getRelationships() {
    return Set.of(SUCCESS, FAILURE, REL_ORIGINAL, REL_EMPTY);
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return List.of(
        SPARQL_SELECT_QUERY, DATA_SOURCE_FORMAT, RECORD_WRITER, RECORD_READER, RDF_PAYLOAD_FIELD);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) {
    final FlowFile original = session.get();
    if (original == null) {
      return;
    }

    final RecordReaderFactory readerFactory =
        context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

    try {

      FlowFile transformed = executeQueryOnRecords(readerFactory, original, context, session);

      if (transformed == null) {
        session.getProvenanceReporter().route(original, REL_EMPTY);
        session.transfer(original, REL_EMPTY);
        return;
      }

      session.getProvenanceReporter().fork(original, List.of(transformed));
      session.transfer(transformed, SUCCESS);
      session.transfer(original, REL_ORIGINAL);

    } catch (Exception e) {
      getLogger().error("Transform failed for {}", original, e);

      session.putAttribute(original, "error.message", e.getMessage());
      session.transfer(original, FAILURE);
    }
  }

  private FlowFile executeQueryOnRecords(
      RecordReaderFactory readerFactory,
      FlowFile original,
      ProcessContext context,
      ProcessSession session)
      throws Exception {

    if (readerFactory == null) {
      return executeQueryOnFlowFileContent(original, context, session);
    }

    RecordSetWriterFactory writerFactory =
        context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

    String query =
        context.getProperty(SPARQL_SELECT_QUERY).evaluateAttributeExpressions(original).getValue();
    String fieldname =
        context.getProperty(RDF_PAYLOAD_FIELD).evaluateAttributeExpressions(original).getValue();
    Lang rdfLang = SparqlProcessorProperties.getDataSourceFormat(context, original);

    final StopWatch stopWatch = new StopWatch(true);

    try (final InputStream in = session.read(original);
        final RecordReader reader = readerFactory.createRecordReader(original, in, getLogger())) {

      // Get first record
      Record incomingRecord = reader.nextRecord();
      if (incomingRecord == null) {
        getLogger().info("{} had no Records to transform", original);
        return null;
      }

      // Query the first record and derive the schema in case the record writer chooses to
      // inherit the record Schema from the record itself.
      ResultSetRewindable firstResults =
          getQueryResults(
              incomingRecord.getAsString(fieldname).getBytes(StandardCharsets.UTF_8),
              rdfLang,
              query);
      while (!firstResults.hasNext()) {
        incomingRecord = reader.nextRecord();
        if (incomingRecord == null) {
          return null;
        }
        firstResults =
            getQueryResults(
                incomingRecord.getAsString(fieldname).getBytes(StandardCharsets.UTF_8),
                rdfLang,
                query);
      }

      RecordSchema writeSchema;
      try {
        writeSchema = writerFactory.getSchema(original.getAttributes(), null);
      } catch (SchemaNotFoundException e) {
        writeSchema = deriveSchema(firstResults);
      }

      RecordSchema finalWriteSchema = writeSchema;
      FlowFile resultingFlowFile =
          writeRecords(
              session,
              getSchemaRecordsListPair(firstResults, writeSchema),
              writeSchema,
              writerFactory,
              original,
              getLogger(),
              reader,
              record ->
                  getSchemaRecordsListPair(
                      getQueryResults(
                          record.getAsString(fieldname).getBytes(StandardCharsets.UTF_8),
                          rdfLang,
                          query),
                      finalWriteSchema));
      session
          .getProvenanceReporter()
          .fork(
              original,
              Collections.singleton(resultingFlowFile),
              "Transformed with query " + query,
              stopWatch.getElapsed(TimeUnit.MILLISECONDS));
      getLogger().debug("Transform completed {}", original);

      return resultingFlowFile;
    }
  }

  private FlowFile executeQueryOnFlowFileContent(
      FlowFile original, ProcessContext context, ProcessSession session) throws Exception {
    RecordSetWriterFactory writerFactory =
        context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

    String query =
        context.getProperty(SPARQL_SELECT_QUERY).evaluateAttributeExpressions(original).getValue();
    Lang rdfLang = SparqlProcessorProperties.getDataSourceFormat(context, original);

    final StopWatch stopWatch = new StopWatch(true);

    ByteArrayOutputStream rdfStream = new ByteArrayOutputStream();
    session.exportTo(original, rdfStream);

    ResultSetRewindable queryResults = getQueryResults(rdfStream.toByteArray(), rdfLang, query);

    RecordSchema writeSchema;
    try {
      writeSchema = writerFactory.getSchema(original.getAttributes(), null);
    } catch (SchemaNotFoundException e) {
      writeSchema = deriveSchema(queryResults);
    }

    List<Record> resultingRecords = getSchemaRecordsListPair(queryResults, writeSchema);

    FlowFile resultingFlowFile =
        writeRecords(session, resultingRecords, writeSchema, writerFactory, original, getLogger());
    session
        .getProvenanceReporter()
        .fork(
            original,
            Collections.singleton(resultingFlowFile),
            "Transformed with query " + query,
            stopWatch.getElapsed(TimeUnit.MILLISECONDS));
    getLogger().debug("Transform completed {}", original);

    return resultingFlowFile;
  }
}
