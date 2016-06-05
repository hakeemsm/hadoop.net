using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Serializer;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestWritableJobConf : TestCase
	{
		private static readonly Configuration Conf = new Configuration();

		/// <exception cref="System.Exception"/>
		private K SerDeser<K>(K conf)
		{
			SerializationFactory factory = new SerializationFactory(Conf);
			Org.Apache.Hadoop.IO.Serializer.Serializer<K> serializer = factory.GetSerializer(
				GenericsUtil.GetClass(conf));
			Deserializer<K> deserializer = factory.GetDeserializer(GenericsUtil.GetClass(conf
				));
			DataOutputBuffer @out = new DataOutputBuffer();
			serializer.Open(@out);
			serializer.Serialize(conf);
			serializer.Close();
			DataInputBuffer @in = new DataInputBuffer();
			@in.Reset(@out.GetData(), @out.GetLength());
			deserializer.Open(@in);
			K after = deserializer.Deserialize(null);
			deserializer.Close();
			return after;
		}

		private void AssertEquals(Configuration conf1, Configuration conf2)
		{
			// We ignore deprecated keys because after deserializing, both the
			// deprecated and the non-deprecated versions of a config are set.
			// This is consistent with both the set and the get methods.
			IEnumerator<KeyValuePair<string, string>> iterator1 = conf1.GetEnumerator();
			IDictionary<string, string> map1 = new Dictionary<string, string>();
			while (iterator1.HasNext())
			{
				KeyValuePair<string, string> entry = iterator1.Next();
				if (!Configuration.IsDeprecated(entry.Key))
				{
					map1[entry.Key] = entry.Value;
				}
			}
			IEnumerator<KeyValuePair<string, string>> iterator2 = conf2.GetEnumerator();
			IDictionary<string, string> map2 = new Dictionary<string, string>();
			while (iterator2.HasNext())
			{
				KeyValuePair<string, string> entry = iterator2.Next();
				if (!Configuration.IsDeprecated(entry.Key))
				{
					map2[entry.Key] = entry.Value;
				}
			}
			NUnit.Framework.Assert.AreEqual(map1, map2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEmptyConfiguration()
		{
			JobConf conf = new JobConf();
			Configuration deser = SerDeser(conf);
			AssertEquals(conf, deser);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNonEmptyConfiguration()
		{
			JobConf conf = new JobConf();
			conf.Set("a", "A");
			conf.Set("b", "B");
			Configuration deser = SerDeser(conf);
			AssertEquals(conf, deser);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestConfigurationWithDefaults()
		{
			JobConf conf = new JobConf(false);
			conf.Set("a", "A");
			conf.Set("b", "B");
			Configuration deser = SerDeser(conf);
			AssertEquals(conf, deser);
		}
	}
}
