package be.vlaanderen.informatievlaanderen.ldes.ldi.requestexecutor.domain.valueobjects;

import org.apache.http.message.BasicHeader;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ResponseTest {

	@Test
	void hasStatus() {
		Response response = new Response(List.of(), 200, null);
		assertTrue(response.hasStatus(200));
		assertFalse(response.hasStatus(100));
	}

	@Test
	void getValueOfHeader() {
		Response response = new Response(List.of(new BasicHeader("location", "value")), 302, null);
		assertEquals("value", response.getFirstHeaderValue("LOCATION").orElseThrow());
		assertEquals("value", response.getFirstHeaderValue("lOcAtIon").orElseThrow());
		assertEquals("value", response.getFirstHeaderValue("location").orElseThrow());
	}

}