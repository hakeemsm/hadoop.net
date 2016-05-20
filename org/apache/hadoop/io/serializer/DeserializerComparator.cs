using Sharpen;

namespace org.apache.hadoop.io.serializer
{
	/// <summary>
	/// <p>
	/// A
	/// <see cref="org.apache.hadoop.io.RawComparator{T}"/>
	/// that uses a
	/// <see cref="Deserializer{T}"/>
	/// to deserialize
	/// the objects to be compared so that the standard
	/// <see cref="System.Collections.IComparer{T}"/>
	/// can
	/// be used to compare them.
	/// </p>
	/// <p>
	/// One may optimize compare-intensive operations by using a custom
	/// implementation of
	/// <see cref="org.apache.hadoop.io.RawComparator{T}"/>
	/// that operates directly
	/// on byte representations.
	/// </p>
	/// </summary>
	/// <?/>
	public abstract class DeserializerComparator<T> : org.apache.hadoop.io.RawComparator
		<T>
	{
		private org.apache.hadoop.io.InputBuffer buffer = new org.apache.hadoop.io.InputBuffer
			();

		private org.apache.hadoop.io.serializer.Deserializer<T> deserializer;

		private T key1;

		private T key2;

		/// <exception cref="System.IO.IOException"/>
		protected internal DeserializerComparator(org.apache.hadoop.io.serializer.Deserializer
			<T> deserializer)
		{
			this.deserializer = deserializer;
			this.deserializer.open(buffer);
		}

		public virtual int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			try
			{
				buffer.reset(b1, s1, l1);
				key1 = deserializer.deserialize(key1);
				buffer.reset(b2, s2, l2);
				key2 = deserializer.deserialize(key2);
			}
			catch (System.IO.IOException e)
			{
				throw new System.Exception(e);
			}
			return compare(key1, key2);
		}

		public abstract int compare(T arg1, T arg2);
	}
}
