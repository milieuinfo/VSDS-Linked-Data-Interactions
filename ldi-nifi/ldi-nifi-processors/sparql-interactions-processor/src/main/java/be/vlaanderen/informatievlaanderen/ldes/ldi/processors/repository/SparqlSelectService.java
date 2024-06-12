package be.vlaanderen.informatievlaanderen.ldes.ldi.processors.repository;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

import java.util.Iterator;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class SparqlSelectService {
    public static ResultSet executeSelectTest(Model inputModel, String queryString) {
        final Query query = QueryFactory.create(queryString);
        try (QueryExecution queryExecution = QueryExecutionFactory.create(query, inputModel)) {
            ResultSet resultSet = queryExecution.execSelect();
            return resultSet;
        }
    }

    public JsonArray executeSelect(Model inputModel, String queryString) {
        return executeSelect(inputModel, queryString, true);
    }
    public JsonArray executeSelect(Model inputModel, String queryString, boolean outputNullValues) {
        final Query query = QueryFactory.create(queryString);
        try (QueryExecution queryExecution = QueryExecutionFactory.create(query, inputModel)) {
            ResultSet resultSet = queryExecution.execSelect();
            return toJsonArray(resultSet, outputNullValues);
        }
    }

    private JsonArray toJsonArray(ResultSet resultSet, boolean outputNullValues) {
        JsonArray result = new JsonArray();
        List<String> resultVars = resultSet.getResultVars();
        while (resultSet.hasNext()) {
            QuerySolution querySolution = resultSet.next();
            result.add(toJsonObject(querySolution, resultVars, outputNullValues));
        }
        return result;
    }

    private JsonObject toJsonObject(QuerySolution querySolution, List<String> resultVars, boolean outputNullValues) {
        JsonObject jsonObject = new JsonObject();
        Iterator<String> varNames = resultVars.listIterator();
        while (varNames.hasNext()) {
            final String key = varNames.next();
            RDFNode node = querySolution.get(key);
            if (node == null) {
                if (outputNullValues) jsonObject.addProperty(key, (String) null);
                continue;
            }

            if (node.isResource()) {
                Resource resourceNode = node.asResource();
                String value = resourceNode.isURIResource() ? resourceNode.getURI() :
                               resourceNode.isAnon() ? resourceNode.getId().getLabelString() : resourceNode.toString();
                jsonObject.addProperty(key, value);
                continue;
            }

            if (!node.isLiteral()) {
                jsonObject.addProperty(key, node.toString());
                continue;
            }

            Literal literal = node.asLiteral();
            Object literalValue = literal.getValue();
            if (literalValue instanceof Number) {
                // XSD numeric values are converted to Numbers and a correct representation.
                // In case of float and double values this will be the decimal value with or without an exponent but the representation can differ from the original lexical form in the data.
                // There are 3 special values: NaN, +Infinity, -Infinity
                jsonObject.addProperty(key, (Number) literalValue);
            }
            else if (literalValue instanceof Boolean) {
                jsonObject.addProperty(key, (Boolean) literalValue);
            }
            else {
//              jsonObject.addProperty(key, literal.getValue().toString()) is not correct. e.g. it does not work if the literal has a custom datatype (e.g. skos:notation)
// 				RDFDatatype literalDatatype = literal.getDatatype();
//				jsonObject.addProperty(key, literalDatatype.unparse(literal.getValue())); // this is ok, but it does convert datetimes with timezones to UTC (time zone Z)
                jsonObject.addProperty(key, literal.getLexicalForm());
            }
        }
        return jsonObject;
    }
}
