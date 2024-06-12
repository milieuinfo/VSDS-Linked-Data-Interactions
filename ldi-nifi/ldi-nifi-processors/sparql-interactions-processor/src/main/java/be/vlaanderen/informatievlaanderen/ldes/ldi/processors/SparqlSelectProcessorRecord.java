package be.vlaanderen.informatievlaanderen.ldes.ldi.processors;

import be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StopWatch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.DATA_SOURCE_FORMAT;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.RDF_PAYLOAD_FIELD;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.RECORD_READER;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.RECORD_WRITER;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.SPARQL_SELECT_QUERY;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.services.FlowManager.FAILURE;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.services.FlowManager.SUCCESS;

/**
// * TODO Extra relation voor empty output
// * TODO Configure custom recordschema and implement mapping from sparql query solutions to records
// * TODO Refactoring
 */
@Tags({"ldes, rdf, SPARQL, record, select"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type",
                         description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
        @WritesAttribute(attribute = "record.count",
                         description = "The number of records in the FlowFile"),
        @WritesAttribute(attribute = "error.message",
                         description = "This attribute provides on failure the error message.")
})
@CapabilityDescription(
        "Reads a FlowFile's RDF data content, executes the provided SPARQL select query on the parsed RDF data and writes the resultset to records in a new flowFile. " +
        "If the incoming flowfile contains records of RDF data.a record reader must be configured and the record field containing the RDF payload.")
public class SparqlSelectProcessorRecord extends AbstractProcessor {

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was transformed. If the FlowFile fails processing, nothing will be sent to this relationship")
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(SUCCESS, FAILURE, REL_ORIGINAL);
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(SPARQL_SELECT_QUERY, DATA_SOURCE_FORMAT, RECORD_WRITER, RECORD_READER, RDF_PAYLOAD_FIELD);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        FlowFile transformed = null;
        try {
            if (readerFactory == null) transformed = transform(original, context, session);
            else transformed = transform(readerFactory, original, context, session);
        }
        catch (Exception e) {
            getLogger().error("Transform failed for {}", original, e);
            session.putAttribute(original, "error.message", e.getMessage());
            e.printStackTrace();

            session.transfer(original, FAILURE);
            if (transformed != null) {
                session.remove(transformed);
            }
            return;
        }
        if (transformed != null) {
            session.transfer(transformed, SUCCESS);
        }
        session.transfer(original, REL_ORIGINAL);
    }

    private FlowFile transform(RecordReaderFactory readerFactory, FlowFile original, ProcessContext context, ProcessSession session) throws Exception {
        RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        String query = context.getProperty(SPARQL_SELECT_QUERY).evaluateAttributeExpressions(original).getValue();
        String fieldname = context.getProperty(RDF_PAYLOAD_FIELD).evaluateAttributeExpressions(original).getValue();
        Lang rdfLang = SparqlProcessorProperties.getDataSourceFormat(context, original);

        final StopWatch stopWatch = new StopWatch(true);
        FlowFile transformed = null;
        try (final InputStream in = session.read(original);
             final RecordReader reader = readerFactory.createRecordReader(original, in, getLogger())) {

            // Get first record
            Record incomingRecord = reader.nextRecord();
            if (incomingRecord == null) {
//                TODO: Wat indien er geen records zijn? Schrijf lege recordset? Indien ja, met welk recordschema? Of maakt dit niets uit?
//                RecordSchema schema = writerFactory.getSchema(original.getAttributes(), reader.getSchema());
                getLogger().info("{} had no Records to transform", original);

                return null;
            }

            Map<String, String> attributes = new HashMap<>();
            WriteResult writeResult;

            transformed = session.create(original);

            // Transform the first record and derive the schema in case the record writer chooses to inherit the record Schema from the record itself.
            Pair<RecordSchema, List<Record>> resultingRecords = transform(context, query, incomingRecord, fieldname, rdfLang);
            final RecordSchema writeSchema = writerFactory.getSchema(original.getAttributes(), resultingRecords.getLeft());
            try (final OutputStream out = session.write(transformed);
                 final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, transformed)) {

                writer.beginRecordSet();

                // TODO Wat indien er voor deze incoming record geen query resultaten zijn? We gaan dit noot detecteren. Schrijf deze records naar andere relatie?
                for (Record queryResult : resultingRecords.getRight()) {
                    writer.write(queryResult);
                }

                while ((incomingRecord = reader.nextRecord()) != null) {
                    resultingRecords = transform(context, query, incomingRecord, fieldname, rdfLang);
                    for (Record queryResult : resultingRecords.getRight()) {
                        writer.write(queryResult);
                    }
                }

                writeResult = writer.finishRecordSet();

                closeQuietly(writer, transformed);

                attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                attributes.putAll(writeResult.getAttributes());
            }

            transformed = session.putAllAttributes(transformed, attributes);
            session.getProvenanceReporter().fork(original, Collections.singleton(transformed), "Transformed with query " + query, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            getLogger().debug("Transform completed {}", original);

            return transformed;
        }
        catch (Exception e) {
            if (transformed != null) session.remove(transformed);
            throw e;
        }
    }

    private FlowFile transform(FlowFile original, ProcessContext context, ProcessSession session) throws Exception {
        RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        String query = context.getProperty(SPARQL_SELECT_QUERY).evaluateAttributeExpressions(original).getValue();
        Lang rdfLang = SparqlProcessorProperties.getDataSourceFormat(context, original);

        final StopWatch stopWatch = new StopWatch(true);

//        Model inputModel = receiveDataAsModel(session, original,
//                SparqlProcessorProperties.getDataSourceFormat(context,original));
//        ResultSet resultSet = SparqlSelectService.executeSelectTest(inputModel, query);
//        boolean test = resultSet.hasNext();

        ByteArrayOutputStream rdfStream = new ByteArrayOutputStream();
        session.exportTo(original, rdfStream);

        Pair<RecordSchema, List<Record>> resultingRecords = transform(context, query, () -> new ByteArrayInputStream(rdfStream.toByteArray()), rdfLang);

        final Map<String, String> attributes = new HashMap<>();

        RecordSchema schema = resultingRecords.getLeft();
        RecordSchema writeSchema = writerFactory.getSchema(original.getAttributes(), schema);
        WriteResult writeResult;

        FlowFile transformed = session.create(original);
        try (final OutputStream out = session.write(transformed);
             final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, transformed)) {

            writer.beginRecordSet();
            for (Record queryrecord : resultingRecords.getRight()) {
                writer.write(queryrecord);
            }
            writeResult = writer.finishRecordSet();

            closeQuietly(writer, transformed);

            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
            attributes.putAll(writeResult.getAttributes());

            transformed = session.putAllAttributes(transformed, attributes);
            session.getProvenanceReporter().fork(original, Collections.singleton(transformed), "Transformed with query " + query, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            getLogger().debug("Transform completed {}", original);

            return transformed;
        }
        catch (Exception e) {
            if (transformed != null) session.remove(transformed);
            throw e;
        }
    }

    private Pair<RecordSchema, List<Record>> transform(final ProcessContext context, String query, Record inputRecord, String fieldname, Lang lang) throws Exception {
        String content = inputRecord.getAsString(fieldname);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

        return transform(context, query, () -> byteArrayInputStream, lang);
    }

    private Pair<RecordSchema, List<Record>> transform(final ProcessContext context, String queryString, Supplier<InputStream> rdfpayload, Lang lang) {
        Model model = RDFParser
                .source(rdfpayload.get())
                .lang(lang)
                .build()
                .toModel();

        return executeSelect(model, queryString, resultSet -> {
            RecordSchema recordSchema = deriveSchema(resultSet);
            List<Record> records = toRecords(resultSet, recordSchema);
            return Pair.of(recordSchema, records);
        });
    }

    public <R> R executeSelect(Model inputModel, String queryString, Function<ResultSet, R> handler) {
        final Query query = QueryFactory.create(queryString);
        try (QueryExecution queryExecution = QueryExecutionFactory.create(query, inputModel)) {
            ResultSet resultSet = queryExecution.execSelect();
            return handler.apply(resultSet);
        }
    }

    private List<Record> toRecords(ResultSet resultSet, RecordSchema recordSchema) {
        List<Record> records = new ArrayList<>();
        List<String> resultVars = resultSet.getResultVars();
        while (resultSet.hasNext()) {
            QuerySolution querySolution = resultSet.next();
            records.add(toRecord(querySolution, resultVars, recordSchema));
        }
        return records;
    }

    private Record toRecord(QuerySolution querySolution, List<String> resultVars, RecordSchema recordSchema) {
        Map<String, Object> valueMap = new HashMap<>();
        for (String varName : resultVars) {
            RDFNode node = querySolution.get(varName);
            if (node == null) {
                valueMap.put(varName, null);
                continue;
            }

            if (node.isResource()) {
                Resource resourceNode = node.asResource();
                String value = resourceNode.isURIResource() ? resourceNode.getURI() :
                               resourceNode.isAnon() ? resourceNode.getId().getLabelString() : resourceNode.toString();
                valueMap.put(varName, value);
                continue;
            }

            if (!node.isLiteral()) {
                valueMap.put(varName, node.toString());
                continue;
            }

            Literal literal = node.asLiteral();
            Object literalValue = literal.getValue();
            if (literalValue instanceof Number) {
                // XSD numeric values are converted to Numbers and a correct representation.
                // In case of float and double values this will be the decimal value with or without an exponent but the representation can differ from the original lexical form in the data.
                // There are 3 special values: NaN, +Infinity, -Infinity
                valueMap.put(varName, literal.getLexicalForm());
            }
            else if (literalValue instanceof Boolean) {
                valueMap.put(varName, literal.getLexicalForm());
            }
            else {
//              jsonObject.addProperty(key, literal.getValue().toString()) is not correct. e.g. it does not work if the literal has a custom datatype (e.g. skos:notation)
// 				RDFDatatype literalDatatype = literal.getDatatype();
//				jsonObject.addProperty(key, literalDatatype.unparse(literal.getValue())); // this is ok, but it does convert datetimes with timezones to UTC (time zone Z)
                valueMap.put(varName, literal.getLexicalForm());
            }
        }
        return new MapRecord(recordSchema, valueMap);
    }

    private RecordSchema deriveSchema(ResultSet resultSet) {
        List<RecordField> recordFields = new ArrayList<>();
        for (String varName : resultSet.getResultVars()) {
            recordFields.add(new RecordField(varName, RecordFieldType.STRING.getDataType()));
        }
        return new SimpleRecordSchema(recordFields);
    }

    private void closeQuietly(RecordSetWriter writer, FlowFile transformed) {
        try {
            writer.close();
        }
        catch (final IOException ioe) {
            getLogger().warn("Failed to close Writer for {}", transformed);
        }
    }

}
