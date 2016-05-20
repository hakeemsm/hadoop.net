using Sharpen;

namespace org.apache.hadoop.io
{
	public class AvroTestUtil
	{
		/// <exception cref="System.Exception"/>
		public static void testReflect(object value, string schema)
		{
			testReflect(value, Sharpen.Runtime.getClassForObject(value), schema);
		}

		/// <exception cref="System.Exception"/>
		public static void testReflect(object value, java.lang.reflect.Type type, string 
			schema)
		{
			// check that schema matches expected
			org.apache.avro.Schema s = ((org.apache.avro.reflect.ReflectData)org.apache.avro.reflect.ReflectData
				.get()).getSchema(type);
			NUnit.Framework.Assert.AreEqual(org.apache.avro.Schema.parse(schema), s);
			// check that value is serialized correctly
			org.apache.avro.reflect.ReflectDatumWriter<object> writer = new org.apache.avro.reflect.ReflectDatumWriter
				<object>(s);
			java.io.ByteArrayOutputStream @out = new java.io.ByteArrayOutputStream();
			writer.write(value, org.apache.avro.io.EncoderFactory.get().directBinaryEncoder(@out
				, null));
			org.apache.avro.reflect.ReflectDatumReader<object> reader = new org.apache.avro.reflect.ReflectDatumReader
				<object>(s);
			object after = reader.read(null, org.apache.avro.io.DecoderFactory.get().binaryDecoder
				(@out.toByteArray(), null));
			NUnit.Framework.Assert.AreEqual(value, after);
		}
	}
}
