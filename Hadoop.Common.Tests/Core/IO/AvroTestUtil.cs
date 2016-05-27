using System.IO;
using Org.Apache.Avro;
using Org.Apache.Avro.IO;
using Org.Apache.Avro.Reflect;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.IO
{
	public class AvroTestUtil
	{
		/// <exception cref="System.Exception"/>
		public static void TestReflect(object value, string schema)
		{
			TestReflect(value, value.GetType(), schema);
		}

		/// <exception cref="System.Exception"/>
		public static void TestReflect(object value, Type type, string schema)
		{
			// check that schema matches expected
			Schema s = ((ReflectData)ReflectData.Get()).GetSchema(type);
			NUnit.Framework.Assert.AreEqual(Schema.Parse(schema), s);
			// check that value is serialized correctly
			ReflectDatumWriter<object> writer = new ReflectDatumWriter<object>(s);
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			writer.Write(value, EncoderFactory.Get().DirectBinaryEncoder(@out, null));
			ReflectDatumReader<object> reader = new ReflectDatumReader<object>(s);
			object after = reader.Read(null, DecoderFactory.Get().BinaryDecoder(@out.ToByteArray
				(), null));
			NUnit.Framework.Assert.AreEqual(value, after);
		}
	}
}
