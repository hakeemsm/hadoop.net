using Sharpen;

namespace org.apache.hadoop.io.serializer
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
		public static K testSerialization<K>(org.apache.hadoop.conf.Configuration conf, K
			 before)
		{
			org.apache.hadoop.io.serializer.SerializationFactory factory = new org.apache.hadoop.io.serializer.SerializationFactory
				(conf);
			org.apache.hadoop.io.serializer.Serializer<K> serializer = factory.getSerializer(
				org.apache.hadoop.util.GenericsUtil.getClass(before));
			org.apache.hadoop.io.serializer.Deserializer<K> deserializer = factory.getDeserializer
				(org.apache.hadoop.util.GenericsUtil.getClass(before));
			org.apache.hadoop.io.DataOutputBuffer @out = new org.apache.hadoop.io.DataOutputBuffer
				();
			serializer.open(@out);
			serializer.serialize(before);
			serializer.close();
			org.apache.hadoop.io.DataInputBuffer @in = new org.apache.hadoop.io.DataInputBuffer
				();
			@in.reset(@out.getData(), @out.getLength());
			deserializer.open(@in);
			K after = deserializer.deserialize(null);
			deserializer.close();
			return after;
		}
	}
}
