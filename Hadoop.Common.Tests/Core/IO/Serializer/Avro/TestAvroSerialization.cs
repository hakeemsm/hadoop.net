using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO.Serializer;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Serializer.Avro
{
	public class TestAvroSerialization : TestCase
	{
		private static readonly Configuration conf = new Configuration();

		/// <exception cref="System.Exception"/>
		public virtual void TestSpecific()
		{
			AvroRecord before = new AvroRecord();
			before.intField = 5;
			AvroRecord after = SerializationTestUtil.TestSerialization(conf, before);
			Assert.Equal(before, after);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReflectPkg()
		{
			Record before = new Record();
			before.x = 10;
			conf.Set(AvroReflectSerialization.AvroReflectPackages, before.GetType().Assembly.
				GetName());
			Record after = SerializationTestUtil.TestSerialization(conf, before);
			Assert.Equal(before, after);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAcceptHandlingPrimitivesAndArrays()
		{
			SerializationFactory factory = new SerializationFactory(conf);
			NUnit.Framework.Assert.IsNull(factory.GetSerializer<byte[]>());
			NUnit.Framework.Assert.IsNull(factory.GetSerializer<byte>());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReflectInnerClass()
		{
			TestAvroSerialization.InnerRecord before = new TestAvroSerialization.InnerRecord(
				);
			before.x = 10;
			conf.Set(AvroReflectSerialization.AvroReflectPackages, before.GetType().Assembly.
				GetName());
			TestAvroSerialization.InnerRecord after = SerializationTestUtil.TestSerialization
				(conf, before);
			Assert.Equal(before, after);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReflect()
		{
			TestAvroSerialization.RefSerializable before = new TestAvroSerialization.RefSerializable
				();
			before.x = 10;
			TestAvroSerialization.RefSerializable after = SerializationTestUtil.TestSerialization
				(conf, before);
			Assert.Equal(before, after);
		}

		public class InnerRecord
		{
			public int x = 7;

			public override int GetHashCode()
			{
				return x;
			}

			public override bool Equals(object obj)
			{
				if (this == obj)
				{
					return true;
				}
				if (obj == null)
				{
					return false;
				}
				if (GetType() != obj.GetType())
				{
					return false;
				}
				TestAvroSerialization.InnerRecord other = (TestAvroSerialization.InnerRecord)obj;
				if (x != other.x)
				{
					return false;
				}
				return true;
			}
		}

		public class RefSerializable : AvroReflectSerializable
		{
			public int x = 7;

			public override int GetHashCode()
			{
				return x;
			}

			public override bool Equals(object obj)
			{
				if (this == obj)
				{
					return true;
				}
				if (obj == null)
				{
					return false;
				}
				if (GetType() != obj.GetType())
				{
					return false;
				}
				TestAvroSerialization.RefSerializable other = (TestAvroSerialization.RefSerializable
					)obj;
				if (x != other.x)
				{
					return false;
				}
				return true;
			}
		}
	}
}
