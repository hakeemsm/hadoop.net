using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO.Serializer
{
	public class SerializationTestUtil
	{
		/// <summary>A utility that tests serialization/deserialization.</summary>
		/// <param name="conf">
		/// configuration to use, "io.serializations" is read to
		/// determine the serialization
		/// </param>
		/// <?/>
		/// <param name="before">item to (de)serialize</param>
		/// <returns>deserialized item</returns>
		/// <exception cref="System.Exception"/>
		public static K TestSerialization<K>(Configuration conf, K before)
		{
			SerializationFactory factory = new SerializationFactory(conf);
			Org.Apache.Hadoop.IO.Serializer.Serializer<K> serializer = factory.GetSerializer(
				GenericsUtil.GetClass(before));
			Deserializer<K> deserializer = factory.GetDeserializer(GenericsUtil.GetClass(before
				));
			DataOutputBuffer @out = new DataOutputBuffer();
			serializer.Open(@out);
			serializer.Serialize(before);
			serializer.Close();
			DataInputBuffer @in = new DataInputBuffer();
			@in.Reset(@out.GetData(), @out.GetLength());
			deserializer.Open(@in);
			K after = deserializer.Deserialize(null);
			deserializer.Close();
			return after;
		}
	}
}
