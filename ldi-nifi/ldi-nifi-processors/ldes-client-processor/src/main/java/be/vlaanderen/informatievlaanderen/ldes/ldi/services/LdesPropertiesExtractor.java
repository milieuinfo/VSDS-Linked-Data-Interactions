package be.vlaanderen.informatievlaanderen.ldes.ldi.services;

import be.vlaanderen.informatievlaanderen.ldes.ldi.domain.valueobjects.LdesProperties;
import be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.executor.RequestExecutor;
import be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.valueobjects.Response;
import com.apicatalog.jsonld.JsonLdOptions;
import com.apicatalog.jsonld.loader.DocumentLoader;
import com.apicatalog.jsonld.loader.HttpLoader;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.time.Duration;
import ldes.client.startingtreenode.RedirectRequestExecutor;
import ldes.client.startingtreenode.domain.valueobjects.RedirectHistory;
import ldes.client.startingtreenode.domain.valueobjects.StartingNodeRequest;
import ldes.client.treenodesupplier.domain.valueobject.LdesMetaData;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;

import java.io.ByteArrayInputStream;
import java.util.Optional;
import org.apache.jena.sparql.util.Context;

import static org.apache.jena.rdf.model.ResourceFactory.createProperty;
import static org.apache.jena.riot.lang.LangJSONLD11.JSONLD_OPTIONS;

public class LdesPropertiesExtractor {

	private static final String LDES = "https://w3id.org/ldes#";
	private static final Property LDES_VERSION_OF = createProperty(LDES, "versionOfPath");
	private static final Property LDES_TIMESTAMP_PATH = createProperty(LDES, "timestampPath");
	private static final Property TREE_SHAPE = createProperty("https://w3id.org/tree#", "shape");
	private final RequestExecutor requestExecutor;

	public LdesPropertiesExtractor(RequestExecutor requestExecutor) {
		this.requestExecutor = requestExecutor;
	}

	private Optional<String> getPropertyValue(Model model, Property property) {
		return model.listStatements(null, property, (Resource) null)
				.nextOptional()
				.map(Statement::getObject)
				.map(RDFNode::asResource)
				.map(Resource::toString);
	}

	public LdesProperties getLdesProperties(LdesMetaData ldesMetaData, boolean needTimestampPath,
			boolean needVersionOfPath,
			boolean needShape) {

		Model model = getModelFromStartingTreeNode(ldesMetaData.getStartingNodeUrl(), ldesMetaData.getLang());

		String timestampPath = getResource(needTimestampPath, model, LDES_TIMESTAMP_PATH);
		String versionOfPath = getResource(needVersionOfPath, model, LDES_VERSION_OF);
		String shape = getResource(needShape, model, TREE_SHAPE);
		return new LdesProperties(timestampPath, versionOfPath, shape);
	}

	private Model getModelFromStartingTreeNode(String url, Lang lang) {
		ProxySelector proxySelector = ProxySelector.of(
				InetSocketAddress.createUnresolved("forwardproxy-on.lb.cumuli.be", 3128));
		HttpClient httpClient = HttpClient.newBuilder()
				.connectTimeout(Duration.ofSeconds(10))
				.proxy(proxySelector)
				.followRedirects(HttpClient.Redirect.ALWAYS)
				.build();

		DocumentLoader documentLoader = new HttpLoader(httpClient);

		JsonLdOptions options = new JsonLdOptions();
		options.setDocumentLoader(documentLoader);
		RedirectRequestExecutor redirectRequestExecutor = new RedirectRequestExecutor(requestExecutor);
		Response response = redirectRequestExecutor
				.execute(new StartingNodeRequest(url, lang, new RedirectHistory()));

		return RDFParser
				.source(response.getBody().map(ByteArrayInputStream::new).orElseThrow())
				.lang(lang).context(
				Context.create().set(JSONLD_OPTIONS,options))
				.build()
				.toModel();
	}

	private String getResource(boolean resourceNeeded, Model model, Property property) {
		return resourceNeeded
				? getResource(model, property).orElseThrow(() -> new LdesPropertyNotFoundException(property.toString()))
				: null;
	}

	public Optional<String> getResource(Model model, Property property) {
		return getPropertyValue(model, property);
	}
}
