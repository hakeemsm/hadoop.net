using Sharpen;

namespace org.apache.hadoop.io.serializer
{
	public class TestWritableSerialization
	{
		private static readonly org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWritableSerialization()
		{
			org.apache.hadoop.io.Text before = new org.apache.hadoop.io.Text("test writable");
			org.apache.hadoop.io.Text after = org.apache.hadoop.io.serializer.SerializationTestUtil
				.testSerialization(conf, before);
			NUnit.Framework.Assert.AreEqual(before, after);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWritableConfigurable()
		{
			//set the configuration parameter
			conf.set(org.apache.hadoop.io.TestGenericWritable.CONF_TEST_KEY, org.apache.hadoop.io.TestGenericWritable
				.CONF_TEST_VALUE);
			//reuse TestGenericWritable inner classes to test 
			//writables that also implement Configurable.
			org.apache.hadoop.io.TestGenericWritable.FooGenericWritable generic = new org.apache.hadoop.io.TestGenericWritable.FooGenericWritable
				();
			generic.setConf(conf);
			org.apache.hadoop.io.TestGenericWritable.Baz baz = new org.apache.hadoop.io.TestGenericWritable.Baz
				();
			generic.set(baz);
			org.apache.hadoop.io.TestGenericWritable.Baz result = org.apache.hadoop.io.serializer.SerializationTestUtil
				.testSerialization(conf, baz);
			NUnit.Framework.Assert.AreEqual(baz, result);
			NUnit.Framework.Assert.IsNotNull(result.getConf());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWritableComparatorJavaSerialization()
		{
			org.apache.hadoop.io.serializer.Serialization ser = new org.apache.hadoop.io.serializer.JavaSerialization
				();
			org.apache.hadoop.io.serializer.Serializer<org.apache.hadoop.io.serializer.TestWritableSerialization.TestWC
				> serializer = ser.getSerializer(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.serializer.TestWritableSerialization.TestWC
				)));
			org.apache.hadoop.io.DataOutputBuffer dob = new org.apache.hadoop.io.DataOutputBuffer
				();
			serializer.open(dob);
			org.apache.hadoop.io.serializer.TestWritableSerialization.TestWC orig = new org.apache.hadoop.io.serializer.TestWritableSerialization.TestWC
				(0);
			serializer.serialize(orig);
			serializer.close();
			org.apache.hadoop.io.serializer.Deserializer<org.apache.hadoop.io.serializer.TestWritableSerialization.TestWC
				> deserializer = ser.getDeserializer(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.serializer.TestWritableSerialization.TestWC
				)));
			org.apache.hadoop.io.DataInputBuffer dib = new org.apache.hadoop.io.DataInputBuffer
				();
			dib.reset(dob.getData(), 0, dob.getLength());
			deserializer.open(dib);
			org.apache.hadoop.io.serializer.TestWritableSerialization.TestWC deser = deserializer
				.deserialize(null);
			deserializer.close();
			NUnit.Framework.Assert.AreEqual(orig, deser);
		}

		[System.Serializable]
		internal class TestWC : org.apache.hadoop.io.WritableComparator
		{
			internal const long serialVersionUID = unchecked((int)(0x4344));

			internal readonly int val;

			internal TestWC()
				: this(7)
			{
			}

			internal TestWC(int val)
			{
				this.val = val;
			}

			public override bool Equals(object o)
			{
				if (o is org.apache.hadoop.io.serializer.TestWritableSerialization.TestWC)
				{
					return ((org.apache.hadoop.io.serializer.TestWritableSerialization.TestWC)o).val 
						== val;
				}
				return false;
			}

			public override int GetHashCode()
			{
				return val;
			}
		}
	}
}
