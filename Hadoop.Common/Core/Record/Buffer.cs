using System;
using System.Text;


namespace Org.Apache.Hadoop.Record
{
	/// <summary>A byte sequence that is used as a Java native type for buffer.</summary>
	/// <remarks>
	/// A byte sequence that is used as a Java native type for buffer.
	/// It is resizable and distinguishes between the count of the sequence and
	/// the current capacity.
	/// </remarks>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://avro.apache.org/"">Avro</a>."
		)]
	public class Buffer : IComparable, ICloneable
	{
		/// <summary>Number of valid bytes in this.bytes.</summary>
		private int count;

		/// <summary>Backing store for Buffer.</summary>
		private byte[] bytes = null;

		/// <summary>Create a zero-count sequence.</summary>
		public Buffer()
		{
			this.count = 0;
		}

		/// <summary>Create a Buffer using the byte array as the initial value.</summary>
		/// <param name="bytes">This array becomes the backing storage for the object.</param>
		public Buffer(byte[] bytes)
		{
			this.bytes = bytes;
			this.count = (bytes == null) ? 0 : bytes.Length;
		}

		/// <summary>Create a Buffer using the byte range as the initial value.</summary>
		/// <param name="bytes">Copy of this array becomes the backing storage for the object.
		/// 	</param>
		/// <param name="offset">offset into byte array</param>
		/// <param name="length">length of data</param>
		public Buffer(byte[] bytes, int offset, int length)
		{
			Copy(bytes, offset, length);
		}

		/// <summary>Use the specified bytes array as underlying sequence.</summary>
		/// <param name="bytes">byte sequence</param>
		public virtual void Set(byte[] bytes)
		{
			this.count = (bytes == null) ? 0 : bytes.Length;
			this.bytes = bytes;
		}

		/// <summary>Copy the specified byte array to the Buffer.</summary>
		/// <remarks>Copy the specified byte array to the Buffer. Replaces the current buffer.
		/// 	</remarks>
		/// <param name="bytes">byte array to be assigned</param>
		/// <param name="offset">offset into byte array</param>
		/// <param name="length">length of data</param>
		public void Copy(byte[] bytes, int offset, int length)
		{
			if (this.bytes == null || this.bytes.Length < length)
			{
				this.bytes = new byte[length];
			}
			System.Array.Copy(bytes, offset, this.bytes, 0, length);
			this.count = length;
		}

		/// <summary>Get the data from the Buffer.</summary>
		/// <returns>The data is only valid between 0 and getCount() - 1.</returns>
		public virtual byte[] Get()
		{
			if (bytes == null)
			{
				bytes = new byte[0];
			}
			return bytes;
		}

		/// <summary>Get the current count of the buffer.</summary>
		public virtual int GetCount()
		{
			return count;
		}

		/// <summary>
		/// Get the capacity, which is the maximum count that could handled without
		/// resizing the backing storage.
		/// </summary>
		/// <returns>The number of bytes</returns>
		public virtual int GetCapacity()
		{
			return this.Get().Length;
		}

		/// <summary>Change the capacity of the backing storage.</summary>
		/// <remarks>
		/// Change the capacity of the backing storage.
		/// The data is preserved if newCapacity
		/// <literal>&gt;=</literal>
		/// getCount().
		/// </remarks>
		/// <param name="newCapacity">The new capacity in bytes.</param>
		public virtual void SetCapacity(int newCapacity)
		{
			if (newCapacity < 0)
			{
				throw new ArgumentException("Invalid capacity argument " + newCapacity);
			}
			if (newCapacity == 0)
			{
				this.bytes = null;
				this.count = 0;
				return;
			}
			if (newCapacity != GetCapacity())
			{
				byte[] data = new byte[newCapacity];
				if (newCapacity < count)
				{
					count = newCapacity;
				}
				if (count != 0)
				{
					System.Array.Copy(this.Get(), 0, data, 0, count);
				}
				bytes = data;
			}
		}

		/// <summary>Reset the buffer to 0 size</summary>
		public virtual void Reset()
		{
			SetCapacity(0);
		}

		/// <summary>
		/// Change the capacity of the backing store to be the same as the current
		/// count of buffer.
		/// </summary>
		public virtual void Truncate()
		{
			SetCapacity(count);
		}

		/// <summary>Append specified bytes to the buffer.</summary>
		/// <param name="bytes">byte array to be appended</param>
		/// <param name="offset">offset into byte array</param>
		/// <param name="length">length of data</param>
		public virtual void Append(byte[] bytes, int offset, int length)
		{
			SetCapacity(count + length);
			System.Array.Copy(bytes, offset, this.Get(), count, length);
			count = count + length;
		}

		/// <summary>Append specified bytes to the buffer</summary>
		/// <param name="bytes">byte array to be appended</param>
		public virtual void Append(byte[] bytes)
		{
			Append(bytes, 0, bytes.Length);
		}

		// inherit javadoc
		public override int GetHashCode()
		{
			int hash = 1;
			byte[] b = this.Get();
			for (int i = 0; i < count; i++)
			{
				hash = (31 * hash) + b[i];
			}
			return hash;
		}

		/// <summary>Define the sort order of the Buffer.</summary>
		/// <param name="other">The other buffer</param>
		/// <returns>
		/// Positive if this is bigger than other, 0 if they are equal, and
		/// negative if this is smaller than other.
		/// </returns>
		public virtual int CompareTo(object other)
		{
			Org.Apache.Hadoop.Record.Buffer right = ((Org.Apache.Hadoop.Record.Buffer)other);
			byte[] lb = this.Get();
			byte[] rb = right.Get();
			for (int i = 0; i < count && i < right.count; i++)
			{
				int a = (lb[i] & unchecked((int)(0xff)));
				int b = (rb[i] & unchecked((int)(0xff)));
				if (a != b)
				{
					return a - b;
				}
			}
			return count - right.count;
		}

		// inherit javadoc
		public override bool Equals(object other)
		{
			if (other is Org.Apache.Hadoop.Record.Buffer && this != other)
			{
				return CompareTo(other) == 0;
			}
			return (this == other);
		}

		// inheric javadoc
		public override string ToString()
		{
			StringBuilder sb = new StringBuilder(2 * count);
			for (int idx = 0; idx < count; idx++)
			{
				sb.Append(char.ForDigit((bytes[idx] & unchecked((int)(0xF0))) >> 4, 16));
				sb.Append(char.ForDigit(bytes[idx] & unchecked((int)(0x0F)), 16));
			}
			return sb.ToString();
		}

		/// <summary>Convert the byte buffer to a string an specific character encoding</summary>
		/// <param name="charsetName">Valid Java Character Set Name</param>
		/// <exception cref="System.IO.UnsupportedEncodingException"/>
		public virtual string ToString(string charsetName)
		{
			return Runtime.GetStringForBytes(this.Get(), 0, this.GetCount(), charsetName
				);
		}

		// inherit javadoc
		/// <exception cref="CloneNotSupportedException"/>
		public virtual object Clone()
		{
			Org.Apache.Hadoop.Record.Buffer result = (Org.Apache.Hadoop.Record.Buffer)base.MemberwiseClone
				();
			result.Copy(this.Get(), 0, this.GetCount());
			return result;
		}
	}
}
