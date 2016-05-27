using System.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Serializer
{
	/// <summary>
	/// <p>
	/// A
	/// <see cref="Org.Apache.Hadoop.IO.RawComparator{T}"/>
	/// that uses a
	/// <see cref="Deserializer{T}"/>
	/// to deserialize
	/// the objects to be compared so that the standard
	/// <see cref="System.Collections.IEnumerator{T}"/>
	/// can
	/// be used to compare them.
	/// </p>
	/// <p>
	/// One may optimize compare-intensive operations by using a custom
	/// implementation of
	/// <see cref="Org.Apache.Hadoop.IO.RawComparator{T}"/>
	/// that operates directly
	/// on byte representations.
	/// </p>
	/// </summary>
	/// <?/>
	public abstract class DeserializerComparator<T> : RawComparator<T>
	{
		private InputBuffer buffer = new InputBuffer();

		private Deserializer<T> deserializer;

		private T key1;

		private T key2;

		/// <exception cref="System.IO.IOException"/>
		protected internal DeserializerComparator(Deserializer<T> deserializer)
		{
			this.deserializer = deserializer;
			this.deserializer.Open(buffer);
		}

		public virtual int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			try
			{
				buffer.Reset(b1, s1, l1);
				key1 = deserializer.Deserialize(key1);
				buffer.Reset(b2, s2, l2);
				key2 = deserializer.Deserialize(key2);
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
			return Compare(key1, key2);
		}

		public abstract int Compare(T arg1, T arg2);
	}
}
