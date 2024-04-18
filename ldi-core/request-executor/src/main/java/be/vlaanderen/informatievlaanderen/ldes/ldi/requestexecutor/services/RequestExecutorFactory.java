package be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.services;

import be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.executor.RequestExecutor;
import be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.executor.clientcredentials.ClientCredentialsConfig;
import be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.executor.edc.EdcRequestExecutor;
import be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.executor.edc.services.TokenService;
import be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.executor.edc.valueobjects.EdcUrlProxy;
import be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.executor.noauth.DefaultConfig;
import be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.executor.noauth.DefaultRequestExecutor;
import org.apache.http.Header;

import java.util.ArrayList;
import java.util.Collection;

public class RequestExecutorFactory {

    private String proxyHost;

    private Integer proxyPort;

    public RequestExecutor createNoAuthExecutor(Collection<Header> headers) {
        if (this.proxyHost != null && this.proxyPort != null) {
            return new DefaultConfig(headers, this.proxyHost, this.proxyPort).createRequestExecutor();
        }
        return new DefaultConfig(headers).createRequestExecutor();
    }

    public RequestExecutor createNoAuthExecutor() {
        return createNoAuthExecutor(new ArrayList<>());
    }

    public RequestExecutor createClientCredentialsExecutor(Collection<Header> headers,
                                                           String clientId,
                                                           String secret,
                                                           String tokenEndpoint,
                                                           String scope) {
        return new ClientCredentialsConfig(headers, clientId, secret, tokenEndpoint, scope).createRequestExecutor();
    }

    public RequestExecutor createClientCredentialsExecutor(String clientId,
                                                           String secret,
                                                           String tokenEndpoint,
                                                           String scope) {
        return createClientCredentialsExecutor(new ArrayList<>(), clientId, secret, tokenEndpoint, scope);
    }

    public RequestExecutor createEdcExecutor(RequestExecutor requestExecutor,
                                             TokenService tokenService,
                                             EdcUrlProxy edcUrlProxy) {
        return new EdcRequestExecutor(requestExecutor, tokenService, edcUrlProxy);
    }

    public void setProxy(String proxyHost, Integer proxyPort) {
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
    }
}
