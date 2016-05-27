using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Serializer
{
	public class TestSerializationFactory
	{
		[NUnit.Framework.Test]
		public virtual void TestSerializerAvailability()
		{
			Configuration conf = new Configuration();
			SerializationFactory factory = new SerializationFactory(conf);
			// Test that a valid serializer class is returned when its present
			NUnit.Framework.Assert.IsNotNull("A valid class must be returned for default Writable Serde"
				, factory.GetSerializer<Writable>());
			NUnit.Framework.Assert.IsNotNull("A valid class must be returned for default Writable serDe"
				, factory.GetDeserializer<Writable>());
			// Test that a null is returned when none can be found.
			NUnit.Framework.Assert.IsNull("A null should be returned if there are no serializers found."
				, factory.GetSerializer<TestSerializationFactory>());
			NUnit.Framework.Assert.IsNull("A null should be returned if there are no deserializers found"
				, factory.GetDeserializer<TestSerializationFactory>());
		}

		[NUnit.Framework.Test]
		public virtual void TestSerializationKeyIsTrimmed()
		{
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.IoSerializationsKey, " org.apache.hadoop.io.serializer.WritableSerialization "
				);
			SerializationFactory factory = new SerializationFactory(conf);
			NUnit.Framework.Assert.IsNotNull("Valid class must be returned", factory.GetSerializer
				<LongWritable>());
		}
	}
}
