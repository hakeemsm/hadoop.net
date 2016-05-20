using Sharpen;

namespace org.apache.hadoop.io.serializer.avro
{
	public class TestAvroSerialization : NUnit.Framework.TestCase
	{
		private static readonly org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		/// <exception cref="System.Exception"/>
		public virtual void testSpecific()
		{
			org.apache.hadoop.io.serializer.avro.AvroRecord before = new org.apache.hadoop.io.serializer.avro.AvroRecord
				();
			before.intField = 5;
			org.apache.hadoop.io.serializer.avro.AvroRecord after = org.apache.hadoop.io.serializer.SerializationTestUtil
				.testSerialization(conf, before);
			NUnit.Framework.Assert.AreEqual(before, after);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testReflectPkg()
		{
			org.apache.hadoop.io.serializer.avro.Record before = new org.apache.hadoop.io.serializer.avro.Record
				();
			before.x = 10;
			conf.set(org.apache.hadoop.io.serializer.avro.AvroReflectSerialization.AVRO_REFLECT_PACKAGES
				, Sharpen.Runtime.getClassForObject(before).getPackage().getName());
			org.apache.hadoop.io.serializer.avro.Record after = org.apache.hadoop.io.serializer.SerializationTestUtil
				.testSerialization(conf, before);
			NUnit.Framework.Assert.AreEqual(before, after);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAcceptHandlingPrimitivesAndArrays()
		{
			org.apache.hadoop.io.serializer.SerializationFactory factory = new org.apache.hadoop.io.serializer.SerializationFactory
				(conf);
			NUnit.Framework.Assert.IsNull(factory.getSerializer<byte[]>());
			NUnit.Framework.Assert.IsNull(factory.getSerializer<byte>());
		}

		/// <exception cref="System.Exception"/>
		public virtual void testReflectInnerClass()
		{
			org.apache.hadoop.io.serializer.avro.TestAvroSerialization.InnerRecord before = new 
				org.apache.hadoop.io.serializer.avro.TestAvroSerialization.InnerRecord();
			before.x = 10;
			conf.set(org.apache.hadoop.io.serializer.avro.AvroReflectSerialization.AVRO_REFLECT_PACKAGES
				, Sharpen.Runtime.getClassForObject(before).getPackage().getName());
			org.apache.hadoop.io.serializer.avro.TestAvroSerialization.InnerRecord after = org.apache.hadoop.io.serializer.SerializationTestUtil
				.testSerialization(conf, before);
			NUnit.Framework.Assert.AreEqual(before, after);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testReflect()
		{
			org.apache.hadoop.io.serializer.avro.TestAvroSerialization.RefSerializable before
				 = new org.apache.hadoop.io.serializer.avro.TestAvroSerialization.RefSerializable
				();
			before.x = 10;
			org.apache.hadoop.io.serializer.avro.TestAvroSerialization.RefSerializable after = 
				org.apache.hadoop.io.serializer.SerializationTestUtil.testSerialization(conf, before
				);
			NUnit.Framework.Assert.AreEqual(before, after);
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
				if (Sharpen.Runtime.getClassForObject(this) != Sharpen.Runtime.getClassForObject(
					obj))
				{
					return false;
				}
				org.apache.hadoop.io.serializer.avro.TestAvroSerialization.InnerRecord other = (org.apache.hadoop.io.serializer.avro.TestAvroSerialization.InnerRecord
					)obj;
				if (x != other.x)
				{
					return false;
				}
				return true;
			}
		}

		public class RefSerializable : org.apache.hadoop.io.serializer.avro.AvroReflectSerializable
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
				if (Sharpen.Runtime.getClassForObject(this) != Sharpen.Runtime.getClassForObject(
					obj))
				{
					return false;
				}
				org.apache.hadoop.io.serializer.avro.TestAvroSerialization.RefSerializable other = 
					(org.apache.hadoop.io.serializer.avro.TestAvroSerialization.RefSerializable)obj;
				if (x != other.x)
				{
					return false;
				}
				return true;
			}
		}
	}
}
