package be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.executor.clientcredentials;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.Validate.notNull;

import be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.executor.RequestExecutor;
import be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.executor.RequestExecutorSupplier;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.builder.api.DefaultApi20;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.github.scribejava.core.oauth2.clientauthentication.ClientAuthentication;
import com.github.scribejava.core.oauth2.clientauthentication.RequestBodyAuthenticationScheme;
import com.github.scribejava.httpclient.apache.ApacheHttpClient;
import java.util.Collection;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

public class ClientCredentialsConfig implements RequestExecutorSupplier {

  private final Collection<Header> headers;
  private final String clientId;
  private final String secret;
  private final String tokenEndpoint;
  private final String scope;

  private final HttpHost proxy;

  public ClientCredentialsConfig(
      Collection<Header> headers,
      String clientId,
      String secret,
      String tokenEndpoint,
      String scope) {
    this.headers = headers;
    this.clientId = notNull(clientId);
    this.secret = notNull(secret);
    this.tokenEndpoint = notNull(tokenEndpoint);
    this.scope = scope;
    this.proxy = null;
  }

  public ClientCredentialsConfig(
      Collection<Header> headers,
      String clientId,
      String secret,
      String tokenEndpoint,
      String scope,
      HttpHost proxy) {
    this.headers = headers;
    this.clientId = notNull(clientId);
    this.secret = notNull(secret);
    this.tokenEndpoint = notNull(tokenEndpoint);
    this.scope = scope;
    this.proxy = proxy;
  }

  public RequestExecutor createRequestExecutor() {
    return new ClientCredentialsRequestExecutor(
        new OAuth20ServiceTokenCacheWrapper(createService()));
  }

  private OAuth20Service createService() {
    final RequestConfig clientConfig = RequestConfig.custom().setRedirectsEnabled(false).build();
    HttpAsyncClientBuilder httpAsyncClientBuilder = HttpAsyncClientBuilder.create()
        .setDefaultRequestConfig(clientConfig)
        .setDefaultHeaders(headers);
    if (proxy != null) {
      httpAsyncClientBuilder.setProxy(proxy);
    }
    final ApacheHttpClient apacheHttpClient =
        new ApacheHttpClient(
            httpAsyncClientBuilder
                .build());
    final DefaultApi20 authorizationApi = createAuthorizationApi(tokenEndpoint);

    final var serviceBuilder =
        new ServiceBuilder(clientId).apiSecret(secret).httpClient(apacheHttpClient);
    if (isNotEmpty(scope)) {
      serviceBuilder.defaultScope(scope);
    }
    return serviceBuilder.build(authorizationApi);
  }

  private DefaultApi20 createAuthorizationApi(String tokenEndpoint) {
    return new DefaultApi20() {
      @Override
      public String getAccessTokenEndpoint() {
        return tokenEndpoint;
      }

      @Override
      protected String getAuthorizationBaseUrl() {
        throw new UnsupportedOperationException("This API doesn't support a Base URL.");
      }

      @Override
      public ClientAuthentication getClientAuthentication() {
        return RequestBodyAuthenticationScheme.instance();
      }
    };
  }
}
