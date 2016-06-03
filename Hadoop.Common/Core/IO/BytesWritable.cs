using System;
using System.Text;
using Org.Apache.Hadoop.IO;

namespace Hadoop.Common.Core.IO
{
	/// <summary>A byte sequence that is usable as a key or value.</summary>
	/// <remarks>
	/// A byte sequence that is usable as a key or value.
	/// It is resizable and distinguishes between the size of the sequence and
	/// the current capacity. The hash function is the front of the md5 of the
	/// buffer. The sort order is the same as memcmp.
	/// </remarks>
	public class BytesWritable : BinaryComparable, WritableComparable<BinaryComparable>
	{
		private const int LengthBytes = 4;

		private static readonly byte[] EmptyBytes = new byte[] {  };

		private int size;

		private byte[] bytes;

		/// <summary>Create a zero-size sequence.</summary>
		public BytesWritable()
			: this(EmptyBytes)
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
		/// <see cref="GetBytes()"/>
		/// for faster access to the underlying array.
		/// </remarks>
		public virtual byte[] CopyBytes()
		{
			byte[] result = new byte[size];
			System.Array.Copy(bytes, 0, result, 0, size);
			return result;
		}

		/// <summary>Get the data backing the BytesWritable.</summary>
		/// <remarks>
		/// Get the data backing the BytesWritable. Please use
		/// <see cref="CopyBytes()"/>
		/// if you need the returned array to be precisely the length of the data.
		/// </remarks>
		/// <returns>The data is only valid between 0 and getLength() - 1.</returns>
		public override byte[] GetBytes()
		{
			return bytes;
		}

		/// <summary>Get the data from the BytesWritable.</summary>
		[Obsolete(@"Use GetBytes() instead.")]
		public virtual byte[] Get()
		{
			return GetBytes();
		}

		/// <summary>Get the current size of the buffer.</summary>
		public override int GetLength()
		{
			return size;
		}

		/// <summary>Get the current size of the buffer.</summary>
		[Obsolete(@"Use GetLength() instead.")]
		public virtual int GetSize()
		{
			return GetLength();
		}

		/// <summary>Change the size of the buffer.</summary>
		/// <remarks>
		/// Change the size of the buffer. The values in the old range are preserved
		/// and any new values are undefined. The capacity is changed if it is
		/// necessary.
		/// </remarks>
		/// <param name="size">The new number of bytes</param>
		public virtual void SetSize(int size)
		{
			if (size > GetCapacity())
			{
				SetCapacity(size * 3 / 2);
			}
			this.size = size;
		}

		/// <summary>
		/// Get the capacity, which is the maximum size that could handled without
		/// resizing the backing storage.
		/// </summary>
		/// <returns>The number of bytes</returns>
		public virtual int GetCapacity()
		{
			return bytes.Length;
		}

		/// <summary>Change the capacity of the backing storage.</summary>
		/// <remarks>
		/// Change the capacity of the backing storage.
		/// The data is preserved.
		/// </remarks>
		/// <param name="new_cap">The new capacity in bytes.</param>
		public virtual void SetCapacity(int new_cap)
		{
			if (new_cap != GetCapacity())
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
		public virtual void Set(BytesWritable newData)
		{
			Set(newData.bytes, 0, newData.size);
		}

		/// <summary>Set the value to a copy of the given byte range</summary>
		/// <param name="newData">the new values to copy in</param>
		/// <param name="offset">the offset in newData to start at</param>
		/// <param name="length">the number of bytes to copy</param>
		public virtual void Set(byte[] newData, int offset, int length)
		{
			SetSize(0);
			SetSize(length);
			System.Array.Copy(newData, offset, bytes, 0, size);
		}

		// inherit javadoc
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			SetSize(0);
			// clear the old data
			SetSize(@in.ReadInt());
			@in.ReadFully(bytes, 0, size);
		}

		// inherit javadoc
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteInt(size);
			@out.Write(bytes, 0, size);
		}

		public override int GetHashCode()
		{
			return base.GetHashCode();
		}

		/// <summary>Are the two byte sequences equal?</summary>
		public override bool Equals(object right_obj)
		{
			if (right_obj is BytesWritable)
			{
				return base.Equals(right_obj);
			}
			return false;
		}

		/// <summary>Generate the stream of bytes as hex pairs separated by ' '.</summary>
		public override string ToString()
		{
			StringBuilder sb = new StringBuilder(3 * size);
			for (int idx = 0; idx < size; idx++)
			{
				// if not the first, put a blank separator in
				if (idx != 0)
				{
					sb.Append(' ');
				}
				string num = Sharpen.Extensions.ToHexString(unchecked((int)(0xff)) & bytes[idx]);
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
		public class Comparator : WritableComparator
		{
			public Comparator()
				: base(typeof(BytesWritable))
			{
			}

			/// <summary>Compare the buffers in serialized form.</summary>
			public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				return CompareBytes(b1, s1 + LengthBytes, l1 - LengthBytes, b2, s2 + LengthBytes, 
					l2 - LengthBytes);
			}
		}

		static BytesWritable()
		{
			// register this comparator
			WritableComparator.Define(typeof(BytesWritable), new BytesWritable.Comparator());
		}
	}
}
