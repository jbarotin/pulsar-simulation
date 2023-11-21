package org.example;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;

public class PulsarConfig {

    public static final String topicName = "persistent://public/default/simulation.test";

    public static final String brokerUrls = "pulsar://localhost:6650";

    public static final String keyField = "key";
    public static final String messageField = "message";
    public static final String processTimeField = "process_time";
    public static String subscriptionName = "simulation-subscription";

    public static GenericSchema<GenericRecord> getSchema(){
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("simulationTestSchema");
        recordSchemaBuilder.field(keyField).type(SchemaType.INT32);
        recordSchemaBuilder.field(messageField).type(SchemaType.INT32);
        recordSchemaBuilder.field(processTimeField).type(SchemaType.INT32);
        return Schema.generic(recordSchemaBuilder.build(SchemaType.AVRO));
    }
}
