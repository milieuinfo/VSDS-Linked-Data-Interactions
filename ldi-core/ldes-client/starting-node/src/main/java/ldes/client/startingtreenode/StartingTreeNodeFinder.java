package ldes.client.startingtreenode;

import static org.apache.jena.riot.lang.LangJSONLD11.JSONLD_OPTIONS;

import be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.executor.RequestExecutor;
import be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.valueobjects.Response;
import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.JsonLdOptions;
import com.apicatalog.jsonld.document.Document;
import com.apicatalog.jsonld.loader.DocumentLoader;
import com.apicatalog.jsonld.loader.DocumentLoaderOptions;
import com.apicatalog.jsonld.loader.HttpLoader;
import jakarta.json.Json;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import ldes.client.startingtreenode.domain.valueobjects.*;
import ldes.client.startingtreenode.exception.StartingNodeNotFoundException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.util.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.List;

public class StartingTreeNodeFinder {

	private final Logger log = LoggerFactory.getLogger(StartingTreeNodeFinder.class);

	private final RedirectRequestExecutor requestExecutor;
	private final List<StartingNodeSpecification> startingNodeSpecifications;

	public StartingTreeNodeFinder(RequestExecutor requestExecutor) {
		this.requestExecutor = new RedirectRequestExecutor(requestExecutor);
		startingNodeSpecifications = List.of(new ViewSpecification(), new TreeNodeSpecification());
	}

	/**
	 * Determines the first node to be queued.
	 *
	 * @param startingNodeRequest
	 *            can contain a collection, view or treeNode.
	 * @return the first node to be queued by the client
	 */
	public StartingTreeNode determineStartingTreeNode(final StartingNodeRequest startingNodeRequest) {
		log.atInfo().log("determineStartingTreeNode for: " + startingNodeRequest.url());
		final Response response = requestExecutor.execute(startingNodeRequest);
		final Model model = getModelFromResponse(startingNodeRequest.lang(), response.getBody().orElseThrow(), startingNodeRequest.url());
		return selectStartingNode(startingNodeRequest, model);
	}

	private Model getModelFromResponse(Lang lang, byte[] responseBody, String baseUrl) {
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

		return RDFParser.source(new ByteArrayInputStream(responseBody)).lang(lang).base(baseUrl).context(
				Context.create().set(JSONLD_OPTIONS,options)).build().toModel();
	}

	private StartingTreeNode selectStartingNode(StartingNodeRequest startingNodeRequest, Model model) {
		log.atInfo().log("Parsing response for: " + startingNodeRequest.url());
		return startingNodeSpecifications
				.stream()
				.filter(startingNodeSpecification -> startingNodeSpecification.test(model))
				.map(startingNodeSpecification -> startingNodeSpecification.extractStartingNode(model))
				.findFirst()
				.orElseThrow(() -> new StartingNodeNotFoundException(startingNodeRequest.url(),
						"Starting Node could not be extracted from model."));
	}

}
