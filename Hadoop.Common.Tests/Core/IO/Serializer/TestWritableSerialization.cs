using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Serializer
{
	public class TestWritableSerialization
	{
		private static readonly Configuration conf = new Configuration();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWritableSerialization()
		{
			Text before = new Text("test writable");
			Text after = SerializationTestUtil.TestSerialization(conf, before);
			NUnit.Framework.Assert.AreEqual(before, after);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWritableConfigurable()
		{
			//set the configuration parameter
			conf.Set(TestGenericWritable.ConfTestKey, TestGenericWritable.ConfTestValue);
			//reuse TestGenericWritable inner classes to test 
			//writables that also implement Configurable.
			TestGenericWritable.FooGenericWritable generic = new TestGenericWritable.FooGenericWritable
				();
			generic.SetConf(conf);
			TestGenericWritable.Baz baz = new TestGenericWritable.Baz();
			generic.Set(baz);
			TestGenericWritable.Baz result = SerializationTestUtil.TestSerialization(conf, baz
				);
			NUnit.Framework.Assert.AreEqual(baz, result);
			NUnit.Framework.Assert.IsNotNull(result.GetConf());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWritableComparatorJavaSerialization()
		{
			Serialization ser = new JavaSerialization();
			Org.Apache.Hadoop.IO.Serializer.Serializer<TestWritableSerialization.TestWC> serializer
				 = ser.GetSerializer(typeof(TestWritableSerialization.TestWC));
			DataOutputBuffer dob = new DataOutputBuffer();
			serializer.Open(dob);
			TestWritableSerialization.TestWC orig = new TestWritableSerialization.TestWC(0);
			serializer.Serialize(orig);
			serializer.Close();
			Deserializer<TestWritableSerialization.TestWC> deserializer = ser.GetDeserializer
				(typeof(TestWritableSerialization.TestWC));
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(dob.GetData(), 0, dob.GetLength());
			deserializer.Open(dib);
			TestWritableSerialization.TestWC deser = deserializer.Deserialize(null);
			deserializer.Close();
			NUnit.Framework.Assert.AreEqual(orig, deser);
		}

		[System.Serializable]
		internal class TestWC : WritableComparator
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
				if (o is TestWritableSerialization.TestWC)
				{
					return ((TestWritableSerialization.TestWC)o).val == val;
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
