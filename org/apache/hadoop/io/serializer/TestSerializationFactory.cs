using Sharpen;

namespace org.apache.hadoop.io.serializer
{
	public class TestSerializationFactory
	{
		[NUnit.Framework.Test]
		public virtual void testSerializerAvailability()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.io.serializer.SerializationFactory factory = new org.apache.hadoop.io.serializer.SerializationFactory
				(conf);
			// Test that a valid serializer class is returned when its present
			NUnit.Framework.Assert.IsNotNull("A valid class must be returned for default Writable Serde"
				, factory.getSerializer<org.apache.hadoop.io.Writable>());
			NUnit.Framework.Assert.IsNotNull("A valid class must be returned for default Writable serDe"
				, factory.getDeserializer<org.apache.hadoop.io.Writable>());
			// Test that a null is returned when none can be found.
			NUnit.Framework.Assert.IsNull("A null should be returned if there are no serializers found."
				, factory.getSerializer<org.apache.hadoop.io.serializer.TestSerializationFactory
				>());
			NUnit.Framework.Assert.IsNull("A null should be returned if there are no deserializers found"
				, factory.getDeserializer<org.apache.hadoop.io.serializer.TestSerializationFactory
				>());
		}

		[NUnit.Framework.Test]
		public virtual void testSerializationKeyIsTrimmed()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set(org.apache.hadoop.fs.CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, " org.apache.hadoop.io.serializer.WritableSerialization "
				);
			org.apache.hadoop.io.serializer.SerializationFactory factory = new org.apache.hadoop.io.serializer.SerializationFactory
				(conf);
			NUnit.Framework.Assert.IsNotNull("Valid class must be returned", factory.getSerializer
				<org.apache.hadoop.io.LongWritable>());
		}
	}
}
