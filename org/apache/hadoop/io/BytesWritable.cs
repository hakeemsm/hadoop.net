using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A byte sequence that is usable as a key or value.</summary>
	/// <remarks>
	/// A byte sequence that is usable as a key or value.
	/// It is resizable and distinguishes between the size of the sequence and
	/// the current capacity. The hash function is the front of the md5 of the
	/// buffer. The sort order is the same as memcmp.
	/// </remarks>
	public class BytesWritable : org.apache.hadoop.io.BinaryComparable, org.apache.hadoop.io.WritableComparable
		<org.apache.hadoop.io.BinaryComparable>
	{
		private const int LENGTH_BYTES = 4;

		private static readonly byte[] EMPTY_BYTES = new byte[] {  };

		private int size;

		private byte[] bytes;

		/// <summary>Create a zero-size sequence.</summary>
		public BytesWritable()
			: this(EMPTY_BYTES)
		{
		}

		/// <summary>Create a BytesWritable using the byte array as the initial value.</summary>
		/// <param name="bytes">This array becomes the backing storage for the object.</param>
		public BytesWritable(byte[] bytes)
			: this(bytes, bytes.Length)
		{
		}

		/// <summary>
		/// Create a BytesWritable using the byte array as the initial value
		/// and length as the length.
		/// </summary>
		/// <remarks>
		/// Create a BytesWritable using the byte array as the initial value
		/// and length as the length. Use this constructor if the array is larger
		/// than the value it represents.
		/// </remarks>
		/// <param name="bytes">This array becomes the backing storage for the object.</param>
		/// <param name="length">The number of bytes to use from array.</param>
		public BytesWritable(byte[] bytes, int length)
		{
			this.bytes = bytes;
			this.size = length;
		}

		/// <summary>Get a copy of the bytes that is exactly the length of the data.</summary>
		/// <remarks>
		/// Get a copy of the bytes that is exactly the length of the data.
		/// See
		/// <see cref="getBytes()"/>
		/// for faster access to the underlying array.
		/// </remarks>
		public virtual byte[] copyBytes()
		{
			byte[] result = new byte[size];
			System.Array.Copy(bytes, 0, result, 0, size);
			return result;
		}

		/// <summary>Get the data backing the BytesWritable.</summary>
		/// <remarks>
		/// Get the data backing the BytesWritable. Please use
		/// <see cref="copyBytes()"/>
		/// if you need the returned array to be precisely the length of the data.
		/// </remarks>
		/// <returns>The data is only valid between 0 and getLength() - 1.</returns>
		public override byte[] getBytes()
		{
			return bytes;
		}

		/// <summary>Get the data from the BytesWritable.</summary>
		[System.ObsoleteAttribute(@"Use getBytes() instead.")]
		public virtual byte[] get()
		{
			return getBytes();
		}

		/// <summary>Get the current size of the buffer.</summary>
		public override int getLength()
		{
			return size;
		}

		/// <summary>Get the current size of the buffer.</summary>
		[System.ObsoleteAttribute(@"Use getLength() instead.")]
		public virtual int getSize()
		{
			return getLength();
		}

		/// <summary>Change the size of the buffer.</summary>
		/// <remarks>
		/// Change the size of the buffer. The values in the old range are preserved
		/// and any new values are undefined. The capacity is changed if it is
		/// necessary.
		/// </remarks>
		/// <param name="size">The new number of bytes</param>
		public virtual void setSize(int size)
		{
			if (size > getCapacity())
			{
				setCapacity(size * 3 / 2);
			}
			this.size = size;
		}

		/// <summary>
		/// Get the capacity, which is the maximum size that could handled without
		/// resizing the backing storage.
		/// </summary>
		/// <returns>The number of bytes</returns>
		public virtual int getCapacity()
		{
			return bytes.Length;
		}

		/// <summary>Change the capacity of the backing storage.</summary>
		/// <remarks>
		/// Change the capacity of the backing storage.
		/// The data is preserved.
		/// </remarks>
		/// <param name="new_cap">The new capacity in bytes.</param>
		public virtual void setCapacity(int new_cap)
		{
			if (new_cap != getCapacity())
			{
				byte[] new_data = new byte[new_cap];
				if (new_cap < size)
				{
					size = new_cap;
				}
				if (size != 0)
				{
					System.Array.Copy(bytes, 0, new_data, 0, size);
				}
				bytes = new_data;
			}
		}

		/// <summary>Set the BytesWritable to the contents of the given newData.</summary>
		/// <param name="newData">the value to set this BytesWritable to.</param>
		public virtual void set(org.apache.hadoop.io.BytesWritable newData)
		{
			set(newData.bytes, 0, newData.size);
		}

		/// <summary>Set the value to a copy of the given byte range</summary>
		/// <param name="newData">the new values to copy in</param>
		/// <param name="offset">the offset in newData to start at</param>
		/// <param name="length">the number of bytes to copy</param>
		public virtual void set(byte[] newData, int offset, int length)
		{
			setSize(0);
			setSize(length);
			System.Array.Copy(newData, offset, bytes, 0, size);
		}

		// inherit javadoc
		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			setSize(0);
			// clear the old data
			setSize(@in.readInt());
			@in.readFully(bytes, 0, size);
		}

		// inherit javadoc
		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeInt(size);
			@out.write(bytes, 0, size);
		}

		public override int GetHashCode()
		{
			return base.GetHashCode();
		}

		/// <summary>Are the two byte sequences equal?</summary>
		public override bool Equals(object right_obj)
		{
			if (right_obj is org.apache.hadoop.io.BytesWritable)
			{
				return base.Equals(right_obj);
			}
			return false;
		}

		/// <summary>Generate the stream of bytes as hex pairs separated by ' '.</summary>
		public override string ToString()
		{
			java.lang.StringBuilder sb = new java.lang.StringBuilder(3 * size);
			for (int idx = 0; idx < size; idx++)
			{
				// if not the first, put a blank separator in
				if (idx != 0)
				{
					sb.Append(' ');
				}
				string num = int.toHexString(unchecked((int)(0xff)) & bytes[idx]);
				// if it is only one digit, add a leading 0.
				if (num.Length < 2)
				{
					sb.Append('0');
				}
				sb.Append(num);
			}
			return sb.ToString();
		}

		/// <summary>A Comparator optimized for BytesWritable.</summary>
		public class Comparator : org.apache.hadoop.io.WritableComparator
		{
			public Comparator()
				: base(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.BytesWritable)
					))
			{
			}

			/// <summary>Compare the buffers in serialized form.</summary>
			public override int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				return compareBytes(b1, s1 + LENGTH_BYTES, l1 - LENGTH_BYTES, b2, s2 + LENGTH_BYTES
					, l2 - LENGTH_BYTES);
			}
		}

		static BytesWritable()
		{
			// register this comparator
			org.apache.hadoop.io.WritableComparator.define(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.BytesWritable)), new org.apache.hadoop.io.BytesWritable.Comparator
				());
		}
	}
}
