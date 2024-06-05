package be.vlaanderen.informatievlaanderen.ldes.ldi.processors;

import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.LdesProcessorProperties.getPostgresPassword;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.LdesProcessorProperties.getPostgresUrl;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.LdesProcessorProperties.getPostgresUsername;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.LdesProcessorProperties.getStatePersistenceStrategy;
import static be.vlaanderen.informatievlaanderen.ldes.ldi.processors.config.LdesProcessorProperties.stateKept;

import java.util.Map;
import ldes.client.treenodesupplier.domain.valueobject.StatePersistence;
import ldes.client.treenodesupplier.domain.valueobject.StatePersistenceStrategy;
import ldes.client.treenodesupplier.repository.sql.postgres.PostgresProperties;
import org.apache.nifi.processor.ProcessContext;

public class StatePersistenceFactory {

  public StatePersistence getStatePersistence(ProcessContext context) {
    StatePersistenceStrategy state = getStatePersistenceStrategy(context);
    Map<String, String> persistenceProperties = Map.of();
    if (state.equals(StatePersistenceStrategy.POSTGRES)) {
      persistenceProperties = createPostgresProperties(context);
    }
    return StatePersistence.from(state, persistenceProperties, context.getName());
  }

  private Map<String, String> createPostgresProperties(ProcessContext context) {
    String url = getPostgresUrl(context);
    String username = getPostgresUsername(context);
    String password = getPostgresPassword(context);
    boolean keepState = stateKept(context);
    return new PostgresProperties(url, username, password, keepState).getProperties();
  }
}
