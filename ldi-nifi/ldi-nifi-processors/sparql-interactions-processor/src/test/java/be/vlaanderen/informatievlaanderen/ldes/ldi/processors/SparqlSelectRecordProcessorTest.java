package be.vlaanderen.informatievlaanderen.ldes.ldi.processors;

import org.apache.jena.riot.Lang;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.parquet.ParquetReader;
import org.apache.nifi.parquet.ParquetRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.xml.XMLRecordSetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.SparqlSelectRecordProcessor.REL_ORIGINAL;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.DATA_SOURCE_FORMAT;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.RDF_PAYLOAD_FIELD;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.RECORD_READER;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.RECORD_WRITER;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties.SPARQL_SELECT_QUERY;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.services.FlowManager.SUCCESS;

/**
 *
 */
public class SparqlSelectRecordProcessorTest {
    private TestRunner testRunner;

    private String selectQuery = "prefix ex:  <http://example.org/ns/terms#> \n" +
                                 "select * \n" +
                                 "where { \n" +
                                 "  ?subject a ?class.\n" +
                                 "  optional {?subject  ex:title ?title. } \n" +
                                 "  optional {?subject  ex:id ?id. } \n" +
                                 "  optional {?subject  ex:date ?date. } \n" +
                                 "  optional {?subject  ex:datetime ?dateTime. } \n" +
                                 "  optional {?subject  ex:number1 ?number1. } \n" +
                                 "  optional {?subject  ex:number2 ?number2. } \n" +
                                 "  optional {?subject  ex:number3 ?number3. } \n" +
                                 "  optional {?subject  ex:integer ?integer. } \n" +
                                 "  optional {?subject  ex:float ?float. } \n" +
                                 "  optional {?subject  ex:decimal ?decimal. } \n" +
                                 "  optional {?subject  ex:boolean ?boolean. } \n" +
                                 "  optional {?subject  ex:unknowndatatype ?unknown. } \n" +
                                 "  optional {?subject  ex:link ?link.} \n" +
                                 "} ORDER BY ?subject \n";

    private String selectQuery2 = "prefix ex:  <http://example.org/ns/terms#> \n" +
                                  "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n" +
                                  "select distinct * \n" +
                                  "where { \n" +
                                  "  ?subject a ?class.\n" +
                                  "  optional {?subject  rdfs:label ?label. } \n" +
                                  "} ORDER BY ?subject \n";

    private String selectQuery3 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                                  "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                                  "PREFIX dct: <http://purl.org/dc/terms/>\n" +
                                  "PREFIX schema: <https://schema.org/>\n" +
                                  "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                                  "PREFIX prov: <http://www.w3.org/ns/prov#>\n" +
                                  "PREFIX sosa: <http://www.w3.org/ns/sosa/>\n" +
                                  "PREFIX time: <http://www.w3.org/2006/time#>\n" +
                                  "PREFIX imjv: <https://data.imjv.omgeving.vlaanderen.be/ns/imjv#>\n" +
                                  "PREFIX wk: <https://data.vlaanderen.be/ns/waterkwaliteit#>\n" +
                                  "\n" +
                                  "\n" +
                                  "select ?member ?memberDatum ?master ?emissie ?jaar ?label ?kenmerk ?kenmerk_type ?kenmerk_label ?agens ?procedure ?result_value ?result_unitCode ?result_unitText\n" +
                                  "where\n" +
                                  "{\n" +
                                  "   ?member a sosa:Observation .\n" +
                                  "   ?member prov:generatedAtTime ?memberDatum .\n" +
                                  "   ?member dct:isVersionOf ?master .\n" +
                                  "   OPTIONAL { ?member sosa:hasFeatureOfInterest ?emissie }.\n" +
                                  "   OPTIONAL { ?member sosa:phenomenonTime ?time.\n" +
                                  "              ?time time:inXSDgYear ?jaar }.\n" +
                                  "   OPTIONAL { ?member rdfs:label ?label}.\n" +
                                  "   OPTIONAL { ?member sosa:hasResult ?result_uri.\n" +
                                  "              OPTIONAL { ?result_uri rdfs:label ?result_label }.\n" +
                                  "              OPTIONAL { ?result_uri schema:value ?result_value }.\n" +
                                  "              OPTIONAL { ?result_uri schema:unitCode ?result_unitCode }.\n" +
                                  "              OPTIONAL { ?result_uri schema:unitText ?result_unitText }.\n" +
                                  "            }.\n" +
                                  "   OPTIONAL { ?member sosa:usedProcedure ?procedure}.\n" +
                                  "   OPTIONAL { ?member sosa:observedProperty ?kenmerk. ## emissievracht agens, concentratie agens, debiet\n" +
                                  "              OPTIONAL { ?kenmerk a ?kenmerk_type.\n" +
                                  "                         FILTER(?kenmerk_type != sosa:ObservableProperty && ?kenmerk_type != sosa:ObservedProperty && ?kenmerk_type != skos:Concept). }.\n" +
                                  "              OPTIONAL { ?kenmerk rdfs:label ?kenmerk_label }.\n" +
                                  "              OPTIONAL { ?kenmerk wk:agens ?agens }.\n" +
                                  "            }.\n" +
                                  "}";


    @BeforeEach
    public void setUp() {
        testRunner = TestRunners.newTestRunner(SparqlSelectRecordProcessor.class);
    }

    @Test
    void testSuccessFlowCsv() throws Exception {
        CSVRecordSetWriter recordSetWriter = new CSVRecordSetWriter();
        testSuccessFlow(selectQuery, Lang.TURTLE.getHeaderString(), recordSetWriter, Collections.emptyMap(), "data_test2.ttl");
    }

        @Test
    void testSuccessFlowJson() throws Exception {
        JsonRecordSetWriter recordSetWriter = new JsonRecordSetWriter();
        testSuccessFlow(selectQuery, Lang.TURTLE.getHeaderString(), recordSetWriter, Collections.emptyMap(), "data_test2.ttl");
    }

        @Test
    void testSuccessFlowXml() throws Exception {
        XMLRecordSetWriter recordSetWriter = new XMLRecordSetWriter();
        testSuccessFlow(selectQuery, Lang.TURTLE.getHeaderString(), recordSetWriter, Map.of("root_tag_name", "results", "record_tag_name", "result"), "data_test2.ttl");
    }

        @Test
    void testSuccessFlowAvro() throws Exception {
        AvroRecordSetWriter recordSetWriter = new AvroRecordSetWriter();
        testSuccessFlow(selectQuery, Lang.TURTLE.getHeaderString(), recordSetWriter, Collections.emptyMap(), "data_test2.ttl");
    }

        @Test
    void testSuccessFlowParquet() throws Exception {
        ParquetRecordSetWriter recordSetWriter = new ParquetRecordSetWriter();
        testSuccessFlow(selectQuery, Lang.TURTLE.getHeaderString(), recordSetWriter, Collections.emptyMap(), "data_test2.ttl");
    }

    void testSuccessFlow(String select, String format, RecordSetWriterFactory recordSetWriterFactory, Map properties, String filename) throws Exception {
        testRunner.setProperty(SPARQL_SELECT_QUERY, select);
        testRunner.setProperty(DATA_SOURCE_FORMAT, format);
        testRunner.setProperty(RECORD_WRITER, RECORD_WRITER.getName());

        testRunner.addControllerService(
                RECORD_WRITER.getName(),
                recordSetWriterFactory,
                properties);

        testRunner.enableControllerService(recordSetWriterFactory);

        testRunner.assertValid();

        testRunner.enqueue(fileNameToFile(filename).toPath());

        testRunner.run();

        testRunner.assertTransferCount(SUCCESS, 1);
        testRunner.assertTransferCount(REL_ORIGINAL, 1);

        MockFlowFile result = testRunner.getFlowFilesForRelationship(SUCCESS).get(0);
        result.getAttributes()
                .entrySet()
                .forEach(e -> System.out.println(e.getKey() + ": " + e.getValue()));

        System.out.print("Content: ");
        System.out.print(result.getContent());
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

        testRunner.addControllerService(
                RECORD_READER.getName(),
                recordReader);
        testRunner.addControllerService(
                RECORD_WRITER.getName(),
                recordSetWriter);

        testRunner.enableControllerService(recordReader);
        testRunner.enableControllerService(recordSetWriter);

        testRunner.assertValid();

        testRunner.enqueue(fileNameToFile("observaties_lucht.parquet").toPath());

        testRunner.run();

        testRunner.assertTransferCount(SUCCESS, 1);
        testRunner.assertTransferCount(REL_ORIGINAL, 1);

        MockFlowFile result = testRunner.getFlowFilesForRelationship(SUCCESS).get(0);
        result.getAttributes()
                .entrySet()
                .forEach(e -> System.out.println(e.getKey() + ": " + e.getValue()));

        System.out.print("Content: ");
        System.out.print(result.getContent());
    }

    private File fileNameToFile(String fileName) throws URISyntaxException {
        return new File(Objects.requireNonNull(getClass().getClassLoader().getResource(fileName)).toURI());
    }
}
