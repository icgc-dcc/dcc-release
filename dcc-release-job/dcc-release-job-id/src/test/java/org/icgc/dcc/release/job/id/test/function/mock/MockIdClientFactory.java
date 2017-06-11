package org.icgc.dcc.release.job.id.test.function.mock;

import org.icgc.dcc.id.client.core.IdClient;
import org.icgc.dcc.id.client.core.IdClientFactory;

/**
 * Created by gguo on 6/9/17.
 */
public class MockIdClientFactory extends IdClientFactory {

    private MockIdClient client;

    public MockIdClientFactory(String serviceUri, String releaseName) {
        super(serviceUri, releaseName);
    }

    @Override
    public IdClient create() {
        return MockIdClientHolder.client;

    }

    private static class MockIdClientHolder{
        static IdClient client = create();
        private static IdClient create() {
            return new MockIdClient();
        }
    }
}
