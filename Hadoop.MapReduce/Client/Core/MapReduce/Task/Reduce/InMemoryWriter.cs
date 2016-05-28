using System;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	public class InMemoryWriter<K, V> : IFile.Writer<K, V>
	{
		private DataOutputStream @out;

		public InMemoryWriter(BoundedByteArrayOutputStream arrayStream)
			: base(null)
		{
			this.@out = new DataOutputStream(new IFileOutputStream(arrayStream));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Append(K key, V value)
		{
			throw new NotSupportedException("InMemoryWriter.append(K key, V value");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Append(DataInputBuffer key, DataInputBuffer value)
		{
			int keyLength = key.GetLength() - key.GetPosition();
			if (keyLength < 0)
			{
				throw new IOException("Negative key-length not allowed: " + keyLength + " for " +
					 key);
			}
			int valueLength = value.GetLength() - value.GetPosition();
			if (valueLength < 0)
			{
				throw new IOException("Negative value-length not allowed: " + valueLength + " for "
					 + value);
			}
			WritableUtils.WriteVInt(@out, keyLength);
			WritableUtils.WriteVInt(@out, valueLength);
			@out.Write(key.GetData(), key.GetPosition(), keyLength);
			@out.Write(value.GetData(), value.GetPosition(), valueLength);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			// Write EOF_MARKER for key/value length
			WritableUtils.WriteVInt(@out, IFile.EofMarker);
			WritableUtils.WriteVInt(@out, IFile.EofMarker);
			// Close the stream 
			@out.Close();
			@out = null;
		}
	}
}
