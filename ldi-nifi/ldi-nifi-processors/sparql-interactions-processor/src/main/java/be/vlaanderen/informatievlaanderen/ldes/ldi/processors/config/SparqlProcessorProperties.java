package be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;

public class SparqlProcessorProperties {

    private SparqlProcessorProperties() {
    }

    public static final Validator RDFLANG_VALIDATOR =
            (subject, value, context) -> {
                if (context.isExpressionLanguageSupported(subject)
                    && context.isExpressionLanguagePresent(value)) {
                    return (new ValidationResult.Builder())
                            .subject(subject)
                            .input(value)
                            .explanation("Expression Language Present")
                            .valid(true)
                            .build();
                }
                else {
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(value)
                            .valid(RDFLanguages.nameToLang(value) != null)
                            .explanation(
                                    String.format(
                                            "'%s' is not a supported RDF language. Refer to org.apache.jena.riot.RDFLanguages for an overview of available data formats.",
                                            subject))
                            .build();
                }
            };

    public static final PropertyDescriptor DATA_SOURCE_FORMAT = new PropertyDescriptor.Builder()
            .name("DATA_SOURCE_FORMAT")
            .displayName("Data source format")
            .required(true)
            .defaultValue(Lang.NQUADS.getHeaderString())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .addValidator(RDFLANG_VALIDATOR)
            .build();

    public static Lang getDataSourceFormat(final ProcessContext context) {
        return RDFLanguages.nameToLang(context.getProperty(DATA_SOURCE_FORMAT).getValue());
    }

    public static Lang getDataSourceFormat(final ProcessContext context, FlowFile flowFile) {
        return RDFLanguages.nameToLang(context.getProperty(DATA_SOURCE_FORMAT).evaluateAttributeExpressions(flowFile).getValue());
    }

    public static final PropertyDescriptor SPARQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("SPARQL_SELECT_QUERY")
            .displayName("SPARQL Select Query")
            .required(true)
            .defaultValue("SELECT ?s ?p ?o WHERE {?s ?p ?o}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_NULL_VALUES = new PropertyDescriptor.Builder()
            .name("OUTPUT_NULL_VALUES")
            .displayName("Output null values")
            .description("Output named variables in the select query that are not present in a query solution as null values in the resulting json.")
            .required(true)
            .defaultValue(Boolean.TRUE.toString())
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDE_ORIGINAL = new PropertyDescriptor.Builder()
            .name("INCLUDE_ORIGINAL")
            .displayName("Include original model")
            .required(true)
            .defaultValue(Boolean.FALSE.toString())
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor SPARQL_CONSTRUCT_QUERY = new PropertyDescriptor.Builder()
            .name("SPARQL_CONSTRUCT_QUERY")
            .displayName("SPARQL Construct Query")
            .required(true)
            .defaultValue("CONSTRUCT ?s ?p ?o WHERE {?s ?p ?o}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_READER =
            new PropertyDescriptor.Builder()
                    .name("record_reader")
                    .displayName("Record Reader")
                    .description("Specifies the Controller Service to use for reading incoming record data")
                    .identifiesControllerService(RecordReaderFactory.class)
                    .required(false)
                    .build();

    public static final PropertyDescriptor RECORD_WRITER =
            new PropertyDescriptor.Builder()
                    .name("record_writer")
                    .displayName("Record Writer")
                    .description("Specifies the Controller Service to use for writing results to a FlowFile.")
                    .identifiesControllerService(RecordSetWriterFactory.class)
                    .required(true)
                    .build();

    public static final PropertyDescriptor RDF_PAYLOAD_FIELD = new PropertyDescriptor.Builder()
            .name("RDF_PAYLOAD_FIELD")
            .displayName("Record field containing RDF payload to query.")
            .dependsOn(RECORD_READER)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();
}
