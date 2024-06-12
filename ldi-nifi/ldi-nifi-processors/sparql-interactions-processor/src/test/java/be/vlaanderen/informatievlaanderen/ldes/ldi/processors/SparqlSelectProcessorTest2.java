package be.vlaanderen.informatievlaanderen.ldes.ldi.processors;

import be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.SparqlProcessorProperties;
import net.minidev.json.parser.JSONParser;
import org.apache.jena.riot.Lang;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Objects;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.services.FlowManager.SUCCESS;
import static net.minidev.json.parser.JSONParser.DEFAULT_PERMISSIVE_MODE;

/**
 *
 */
public class SparqlSelectProcessorTest2 {

    private static final JSONParser parser = new JSONParser(DEFAULT_PERMISSIVE_MODE);
    private TestRunner testRunner;

    @BeforeEach
    public void setUp() {
        testRunner = TestRunners.newTestRunner(SparqlSelectProcessor.class);
    }

//    @Test
    public void testExecuteSparqlQuery() throws Exception {
        String selectQuery = "prefix ex:  <http://example.org/ns/terms#> \n" +
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
                             "  optional {?subject  ex:link ?link.} \n" +
                             "} ORDER BY ?subject \n";

        testSuccessFlow(selectQuery, "data_test2.ttl");
    }

    private void testSuccessFlow(String selectQuery, String inputFileName)
    throws Exception {
        testRunner.setProperty(SparqlProcessorProperties.SPARQL_SELECT_QUERY, selectQuery);
        testRunner.setProperty(SparqlProcessorProperties.DATA_SOURCE_FORMAT, Lang.TURTLE.getHeaderString());
        testRunner.enqueue(fileNameToFile(inputFileName).toPath());

        testRunner.run();

        MockFlowFile result = testRunner.getFlowFilesForRelationship(SUCCESS).get(0);
        testRunner.assertAllFlowFilesTransferred(SUCCESS);

        result.getAttributes()
                .entrySet()
                .forEach(e -> System.out.println(e.getKey() + ": " + e.getValue()));

        System.out.print(prettyPrint(result.getContent()));
    }

    private String prettyPrint(String jsonContent) {
//        return jsonContent;
        Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
        JsonElement jsonElement = JsonParser.parseString(jsonContent);
        String prettyJsonString = gson.toJson(jsonElement);
        return prettyJsonString;
    }

    private File fileNameToFile(String fileName) throws URISyntaxException {
        return new File(Objects.requireNonNull(getClass().getClassLoader().getResource(fileName)).toURI());
    }
}
