package be.vlaanderen.informatievlaanderen.ldes.ldi.processors;

import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.SparqlSelectRecordProcessor.REL_ORIGINAL;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.DATA_SOURCE_FORMAT;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.RDF_PAYLOAD_FIELD;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.RECORD_READER;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.RECORD_WRITER;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.SPARQL_SELECT_QUERY;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.services.FlowManager.SUCCESS;
import static org.apache.nifi.json.JsonRecordSetWriter.ALLOW_SCIENTIFIC_NOTATION;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.jena.riot.Lang;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.avro.NonCachingDatumReader;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.parquet.ParquetReader;
import org.apache.nifi.parquet.ParquetRecordSetWriter;
import org.apache.nifi.parquet.stream.NifiParquetInputFile;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.xml.XMLRecordSetWriter;
import org.apache.parquet.avro.AvroParquetReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/** */
public class SparqlSelectRecordProcessorTest {
  private TestRunner testRunner;

  private String selectQuery =
      "prefix ex:  <http://example.org/ns/terms#> \n"
          + "select * \n"
          + "where { \n"
          + "  ?subject a ?class.\n"
          + "  optional {?subject  ex:title ?title. } \n"
          + "  optional {?subject  ex:id ?id. } \n"
          + "  optional {?subject  ex:date ?date. } \n"
          + "  optional {?subject  ex:datetime ?dateTime. } \n"
          + "  optional {?subject  ex:number1 ?number1. } \n"
          + "  optional {?subject  ex:number2 ?number2. } \n"
          + "  optional {?subject  ex:number3 ?number3. } \n"
          + "  optional {?subject  ex:integer ?integer. } \n"
          + "  optional {?subject  ex:float ?float. } \n"
          + "  optional {?subject  ex:decimal ?decimal. } \n"
          + "  optional {?subject  ex:boolean ?boolean. } \n"
          + "  optional {?subject  ex:unknowndatatype ?unknown. } \n"
          + "  optional {?subject  ex:link ?link.} \n"
          + "} ORDER BY ?subject \n";

  private String schema =
      """
        {
          "type" : "record",
          "name" : "nifiRecord",
          "namespace" : "org.apache.nifi",
          "fields" : [ {
            "name" : "id",
            "type" : [ "string", "null" ]
          }, {
            "name" : "link",
            "type" : [ "string", "null" ]
          }, {
            "name" : "class",
            "type" : [ "string", "null" ]
          }, {
            "name" : "boolean",
            "type" : [ "boolean", "null" ]
          }, {
            "name" : "dateTime",
            "type" : [ {
              "type" : "long",
              "logicalType" : "timestamp-millis"
            }, "null" ]
          }, {
            "name" : "float",
            "type" : [ "float", "null" ]
          }, {
            "name" : "subject",
            "type" : [ "string", "null" ]
          }, {
            "name" : "number1",
            "type" : [ "double", "null" ]
          }, {
            "name" : "title",
            "type" : [ "string", "null" ]
          }, {
            "name" : "number2",
            "type" : [ "long", "null" ]
          }, {
            "name" : "integer",
            "type" : [ "long", "null" ]
          }, {
            "name" : "number3",
            "type" : [ "double", "null" ]
          }, {
            "name" : "unknown",
            "type" : [ "string", "null" ]
          }, {
            "name" : "decimal",
            "type" : [ {
              "type": "double",
              "logicalType" : "decimal"
            }, "null" ]
          }, {
            "name" : "date",
            "type" : [ {
              "type" : "int",
              "logicalType" : "date"
            }, "null" ]
          } ]
        }
  """;

  private String selectQuery2 =
      "prefix ex:  <http://example.org/ns/terms#> \n"
          + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"
          + "select distinct * \n"
          + "where { \n"
          + "  ?subject a ?class.\n"
          + "  optional {?subject  rdfs:label ?label. } \n"
          + "} ORDER BY ?subject \n";

  private String selectQuery3 =
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
          + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
          + "PREFIX dct: <http://purl.org/dc/terms/>\n"
          + "PREFIX schema: <https://schema.org/>\n"
          + "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n"
          + "PREFIX prov: <http://www.w3.org/ns/prov#>\n"
          + "PREFIX sosa: <http://www.w3.org/ns/sosa/>\n"
          + "PREFIX time: <http://www.w3.org/2006/time#>\n"
          + "PREFIX imjv: <https://data.imjv.omgeving.vlaanderen.be/ns/imjv#>\n"
          + "PREFIX wk: <https://data.vlaanderen.be/ns/waterkwaliteit#>\n"
          + "\n"
          + "\n"
          + "select ?member ?memberDatum ?master ?emissie ?jaar ?label ?kenmerk ?kenmerk_type ?kenmerk_label ?agens ?procedure ?result_value ?result_unitCode ?result_unitText\n"
          + "where\n"
          + "{\n"
          + "   ?member a sosa:Observation .\n"
          + "   ?member prov:generatedAtTime ?memberDatum .\n"
          + "   ?member dct:isVersionOf ?master .\n"
          + "   OPTIONAL { ?member sosa:hasFeatureOfInterest ?emissie }.\n"
          + "   OPTIONAL { ?member sosa:phenomenonTime ?time.\n"
          + "              ?time time:inXSDgYear ?jaar }.\n"
          + "   OPTIONAL { ?member rdfs:label ?label}.\n"
          + "   OPTIONAL { ?member sosa:hasResult ?result_uri.\n"
          + "              OPTIONAL { ?result_uri rdfs:label ?result_label }.\n"
          + "              OPTIONAL { ?result_uri schema:value ?result_value }.\n"
          + "              OPTIONAL { ?result_uri schema:unitCode ?result_unitCode }.\n"
          + "              OPTIONAL { ?result_uri schema:unitText ?result_unitText }.\n"
          + "            }.\n"
          + "   OPTIONAL { ?member sosa:usedProcedure ?procedure}.\n"
          + "   OPTIONAL { ?member sosa:observedProperty ?kenmerk. ## emissievracht agens, concentratie agens, debiet\n"
          + "              OPTIONAL { ?kenmerk a ?kenmerk_type.\n"
          + "                         FILTER(?kenmerk_type != sosa:ObservableProperty && ?kenmerk_type != sosa:ObservedProperty && ?kenmerk_type != skos:Concept). }.\n"
          + "              OPTIONAL { ?kenmerk rdfs:label ?kenmerk_label }.\n"
          + "              OPTIONAL { ?kenmerk wk:agens ?agens }.\n"
          + "            }.\n"
          + "}";

  @BeforeEach
  public void setUp() {
    testRunner = TestRunners.newTestRunner(SparqlSelectRecordProcessor.class);
  }

  @Test
  void testSuccessFlowCsv() throws Exception {

    // when
    CSVRecordSetWriter recordSetWriter = new CSVRecordSetWriter();
    executeRunner(
        selectQuery,
        Lang.TURTLE.getHeaderString(),
        recordSetWriter,
        Collections.emptyMap(),
        "data_test2.ttl");

    // then
    assertSucces();
  }

  @Test
  void testSuccessFlowJson() throws Exception {

    // when
    JsonRecordSetWriter recordSetWriter = new JsonRecordSetWriter();
    executeRunner(
        selectQuery,
        Lang.TURTLE.getHeaderString(),
        recordSetWriter,
        Map.of(ALLOW_SCIENTIFIC_NOTATION.getName(), "true"),
        "data_test2.ttl");

    // then
    assertSucces();

    MockFlowFile result = testRunner.getFlowFilesForRelationship(SUCCESS).get(0);
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonNode = objectMapper.readTree(result.getContent());
    assertThat(jsonNode.isArray()).isTrue();
    assertThat(jsonNode.size()).isEqualTo(6);
    for (Iterator<JsonNode> it = jsonNode.elements(); it.hasNext(); ) {
      JsonNode n = it.next();
      assertThat(n.get("unknown").asText("")).isIn("some lexicalform", "");
    }
  }

  @Test
  void testSuccessFlowXml() throws Exception {

    // when
    XMLRecordSetWriter recordSetWriter = new XMLRecordSetWriter();
    executeRunner(
        selectQuery,
        Lang.TURTLE.getHeaderString(),
        recordSetWriter,
        Map.of("root_tag_name", "results", "record_tag_name", "result"),
        "data_test2.ttl");

    // then
    assertSucces();
    MockFlowFile result = testRunner.getFlowFilesForRelationship(SUCCESS).get(0);

    DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = builderFactory.newDocumentBuilder();
    Document xmlDocument =
        builder.parse(
            new ByteArrayInputStream(result.getContent().getBytes(StandardCharsets.UTF_8)));

    File file = new File("data.xml");
    Files.write(file.toPath(), result.getContentStream().readAllBytes());

    XPath xPath = XPathFactory.newInstance().newXPath();
    String expression1 = "/results/result/unknown";
    NodeList nodeList1 =
        (NodeList) xPath.compile(expression1).evaluate(xmlDocument, XPathConstants.NODESET);
    assertThat(nodeList1.getLength()).isEqualTo(6);

    for (int i = 0; i < nodeList1.getLength(); i++) {
      Node item = nodeList1.item(i);
      assertThat(item.getTextContent()).isIn("some lexicalform", "");
    }
  }

  @Test
  void testSuccessFlowAvroWithoutSchema() throws Exception {

    // when
    AvroRecordSetWriter recordSetWriter = new AvroRecordSetWriter();
    executeRunner(
        selectQuery, Lang.TURTLE.getHeaderString(), recordSetWriter, Map.of(), "data_test2.ttl");

    // then
    assertSucces();
    MockFlowFile result = testRunner.getFlowFilesForRelationship(SUCCESS).get(0);

    DataFileStream<GenericRecord> avroStream =
        new DataFileStream<>(result.getContentStream(), new NonCachingDatumReader<>());

    File file = new File("data.avro");
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>();
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(new Schema.Parser().parse(schema), file);

    while (avroStream.hasNext()) {
      GenericRecord nextRecord = avroStream.next();

      dataFileWriter.append(nextRecord);
      assertThat(nextRecord.get("unknown") == null ? "" : nextRecord.get("unknown").toString())
          .isIn("some lexicalform", "");
      assertThat(nextRecord.get("date") instanceof Integer).isTrue();
      assertThat(nextRecord.get("dateTime") instanceof Long).isTrue();
    }
    dataFileWriter.close();
  }

  @Test
  void testSuccessFlowAvroWithSchema() throws Exception {

    // when
    AvroRecordSetWriter recordSetWriter = new AvroRecordSetWriter();
    executeRunner(
        selectQuery,
        Lang.TURTLE.getHeaderString(),
        recordSetWriter,
        Map.of(
            SCHEMA_ACCESS_STRATEGY.getName(),
            SCHEMA_TEXT_PROPERTY.getValue(),
            SCHEMA_TEXT.getName(),
            schema),
        "data_test2.ttl");

    // then
    assertSucces();
    MockFlowFile result = testRunner.getFlowFilesForRelationship(SUCCESS).get(0);

    DataFileStream<GenericRecord> avroStream =
        new DataFileStream<>(result.getContentStream(), new NonCachingDatumReader<>());

    File file = new File("data.avro");
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>();
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(new Schema.Parser().parse(schema), file);

    while (avroStream.hasNext()) {
      GenericRecord nextRecord = avroStream.next();

      dataFileWriter.append(nextRecord);
      assertThat(nextRecord.get("unknown") == null ? "" : nextRecord.get("unknown").toString())
          .isIn("some lexicalform", "");
      assertThat(nextRecord.get("date") instanceof Integer).isTrue();
      assertThat(nextRecord.get("dateTime") instanceof Long).isTrue();
    }
    dataFileWriter.close();
  }

  @Test
  void testSuccessFlowParquetWithSchema() throws Exception {

    // when
    ParquetRecordSetWriter recordSetWriter = new ParquetRecordSetWriter();
    executeRunner(
        selectQuery,
        Lang.TURTLE.getHeaderString(),
        recordSetWriter,
        Map.of(
            SCHEMA_ACCESS_STRATEGY.getName(),
            SCHEMA_TEXT_PROPERTY.getValue(),
            SCHEMA_TEXT.getName(),
            schema),
        "data_test2.ttl");

    // then
    assertSucces();
    MockFlowFile result = testRunner.getFlowFilesForRelationship(SUCCESS).get(0);

    org.apache.parquet.hadoop.ParquetReader<GenericRecord> genericRecordParquetReader =
        AvroParquetReader.genericRecordReader(
            new NifiParquetInputFile(result.getContentStream(), result.getSize()));

    File file = new File("data.parquet");
    Files.write(file.toPath(), result.getContentStream().readAllBytes());

    while (true) {
      GenericRecord nextRecord = genericRecordParquetReader.read();
      if (nextRecord == null) break;

      assertThat(nextRecord.get("unknown") == null ? "" : nextRecord.get("unknown").toString())
          .isIn("some lexicalform", "");
      assertThat(nextRecord.get("date") instanceof Integer).isTrue();
      assertThat(nextRecord.get("dateTime") instanceof Long).isTrue();
    }
  }

  @Test
  void testSuccessFlowParquetWithoutSchema() throws Exception {

    // when
    ParquetRecordSetWriter recordSetWriter = new ParquetRecordSetWriter();
    executeRunner(
        selectQuery, Lang.TURTLE.getHeaderString(), recordSetWriter, Map.of(), "data_test2.ttl");

    // then
    assertSucces();
    MockFlowFile result = testRunner.getFlowFilesForRelationship(SUCCESS).get(0);

    org.apache.parquet.hadoop.ParquetReader<GenericRecord> genericRecordParquetReader =
        AvroParquetReader.genericRecordReader(
            new NifiParquetInputFile(result.getContentStream(), result.getSize()));

    File file = new File("data.parquet");
    Files.write(file.toPath(), result.getContentStream().readAllBytes());

    while (true) {
      GenericRecord nextRecord = genericRecordParquetReader.read();
      if (nextRecord == null) break;

      assertThat(nextRecord.get("unknown") == null ? "" : nextRecord.get("unknown").toString())
          .isIn("some lexicalform", "");
      assertThat(nextRecord.get("date") instanceof Integer).isTrue();
      assertThat(nextRecord.get("dateTime") instanceof Long).isTrue();
    }
  }

  private void executeRunner(
      String select,
      String format,
      RecordSetWriterFactory recordSetWriterFactory,
      Map properties,
      String filename)
      throws InitializationException, URISyntaxException, IOException {
    testRunner.setProperty(SPARQL_SELECT_QUERY, select);
    testRunner.setProperty(DATA_SOURCE_FORMAT, format);
    testRunner.setProperty(RECORD_WRITER, RECORD_WRITER.getName());

    testRunner.addControllerService(RECORD_WRITER.getName(), recordSetWriterFactory, properties);

    testRunner.enableControllerService(recordSetWriterFactory);

    testRunner.assertValid();

    testRunner.enqueue(fileNameToFile(filename).toPath());

    testRunner.run();
  }

  private void assertSucces() {
    try {
      testRunner.assertTransferCount(SUCCESS, 1);
      testRunner.assertTransferCount(REL_ORIGINAL, 1);
    } catch (AssertionFailedError e) {
      if (!testRunner.getFlowFilesForRelationship("failure").isEmpty()) {
        System.err.println(
            testRunner
                .getFlowFilesForRelationship("failure")
                .get(0)
                .getAttributes()
                .get("error.message"));
      }
      if (!testRunner.getFlowFilesForRelationship("empty").isEmpty()) {
        System.err.println("Resulting flowfile was empty");
      }
      throw e;
    }
  }

  @Test
  void testSuccessFlowParquetInput() throws Exception {
    CSVRecordSetWriter recordSetWriter = new CSVRecordSetWriter();
    ParquetReader recordReader = new ParquetReader();

    testRunner.setProperty(SPARQL_SELECT_QUERY, selectQuery3);
    testRunner.setProperty(DATA_SOURCE_FORMAT, Lang.NQUADS.getHeaderString());
    testRunner.setProperty(RECORD_WRITER, RECORD_WRITER.getName());
    testRunner.setProperty(RECORD_READER, RECORD_READER.getName());
    testRunner.setProperty(RDF_PAYLOAD_FIELD, "payload");

    testRunner.addControllerService(RECORD_READER.getName(), recordReader);
    testRunner.addControllerService(RECORD_WRITER.getName(), recordSetWriter);

    testRunner.enableControllerService(recordReader);
    testRunner.enableControllerService(recordSetWriter);

    testRunner.assertValid();

    testRunner.enqueue(fileNameToFile("observaties_lucht.parquet").toPath());

    testRunner.run();

    assertSucces();

    MockFlowFile result = testRunner.getFlowFilesForRelationship(SUCCESS).get(0);
    result
        .getAttributes()
        .entrySet()
        .forEach(e -> System.out.println(e.getKey() + ": " + e.getValue()));

    System.out.print("Content: ");
    System.out.print(result.getContent());
  }

  private File fileNameToFile(String fileName) throws URISyntaxException {
    return new File(
        Objects.requireNonNull(getClass().getClassLoader().getResource(fileName)).toURI());
  }
}
