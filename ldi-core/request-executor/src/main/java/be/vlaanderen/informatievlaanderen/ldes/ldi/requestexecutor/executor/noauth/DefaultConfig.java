package be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.executor.noauth;

import be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.executor.RequestExecutor;
import be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.executor.RequestExecutorSupplier;
import java.util.Collection;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.impl.client.HttpClientBuilder;

public class DefaultConfig implements RequestExecutorSupplier {

  private final Collection<Header> headers;

  private final HttpHost proxy;

  public DefaultConfig(Collection<Header> headers) {
    this.headers = headers;
    this.proxy = null;
  }

  public DefaultConfig(Collection<Header> headers, String proxyHost, int proxyPort) {
    this.headers = headers;
    this.proxy = new HttpHost(proxyHost, proxyPort);
  }

  public RequestExecutor createRequestExecutor() {
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create().setDefaultHeaders(headers)
        .disableRedirectHandling();
    if (this.proxy != null) {
      httpClientBuilder.setProxy(proxy);
    }
    return new DefaultRequestExecutor(httpClientBuilder.build());
  }

}
