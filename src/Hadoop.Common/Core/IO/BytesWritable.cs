using System;
using System.IO;
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
	public class BytesWritable : BinaryComparable, IWritableComparable<BinaryComparable>
	{
		private const int LengthBytes = 4;

		private static readonly byte[] EmptyBytes = {  };

		private int _size;

		private byte[] _bytes;

		/// <summary>Create a zero-size sequence.</summary>
		public BytesWritable() : this(EmptyBytes)
		{
		}

		/// <summary>Create a BytesWritable using the byte array as the initial value.</summary>
		/// <param name="bytes">This array becomes the backing storage for the object.</param>
		public BytesWritable(byte[] bytes) : this(bytes, bytes.Length)
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
			this._bytes = bytes;
			this._size = length;
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
			byte[] result = new byte[_size];
			System.Array.Copy(_bytes, 0, result, 0, _size);
			return result;
		}

        /// <summary>Get the data backing the BytesWritable.</summary>
        /// <remarks>
        /// Get the data backing the BytesWritable. Please use
        /// <see cref="CopyBytes()"/>
        /// if you need the returned array to be precisely the length of the data.
        /// </remarks>
        /// <returns>The data is only valid between 0 and getLength() - 1.</returns>
        public override byte[] Bytes
        {
            get
            {
                return _bytes;
            }
        }

        /// <summary>Get the data from the BytesWritable.</summary>
        [Obsolete(@"Use GetBytes() instead.")]
        public virtual byte[] Get => Bytes;

	    /// <summary>Get the current size of the buffer.</summary>
        public override int Length => _size;

        /// <summary>Get the current size of the buffer.</summary>

        /// <summary>Change the size of the buffer.</summary>
        /// <remarks>
        /// Change the size of the buffer. The values in the old range are preserved
        /// and any new values are undefined. The capacity is changed if it is
        /// necessary.
        /// </remarks>
        /// <param name="size">The new number of bytes</param>
        [Obsolete(@"Use Length property instead.")]
        public virtual int Size
        {
            get
            {
                return Length;
            }

            set
            {
                if (value > Capacity)
                {
                    Capacity = value * 3 / 2;
                }
                this._size = value;
            }
        }

        /// <summary>
        /// Get the capacity, which is the maximum size that could handled without
        /// resizing the backing storage.
        /// </summary>
        /// <returns>The number of bytes</returns>

        /// <summary>Change the capacity of the backing storage.</summary>
        /// <remarks>
        /// Change the capacity of the backing storage.
        /// The data is preserved.
        /// </remarks>
        /// <param name="new_cap">The new capacity in bytes.</param>
        public virtual int Capacity
        {
            get
            {
                return _bytes.Length;
            }

            set
            {
                if (value != Capacity)
                {
                    byte[] new_data = new byte[value];
                    if (value < _size)
                    {
                        _size = value;
                    }
                    if (_size != 0)
                    {
                        System.Array.Copy(_bytes, 0, new_data, 0, _size);
                    }
                    _bytes = new_data;
                }
            }
        }

        /// <summary>Set the BytesWritable to the contents of the given newData.</summary>
        /// <param name="newData">the value to set this BytesWritable to.</param>
        public virtual void Set(BytesWritable newData)
		{
			Set(newData._bytes, 0, newData._size);
		}

		/// <summary>Set the value to a copy of the given byte range</summary>
		/// <param name="newData">the new values to copy in</param>
		/// <param name="offset">the offset in newData to start at</param>
		/// <param name="length">the number of bytes to copy</param>
		public virtual void Set(byte[] newData, int offset, int length)
		{
			Size = 0;
			Size = length;
			System.Array.Copy(newData, offset, _bytes, 0, _size);
		}

		// inherit javadoc
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader redaer)
		{
			Size = 0;
			// clear the old data
			Size = redaer.Read();
			_bytes = redaer.ReadBytes(_size);
		}

		// inherit javadoc
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter writer)
		{
			writer.Write(_size);
			writer.Write(_bytes, 0, _size);
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
			StringBuilder sb = new StringBuilder(3 * _size);
			for (int idx = 0; idx < _size; idx++)
			{
				// if not the first, put a blank separator in
				if (idx != 0)
				{
					sb.Append(' ');
				}
				string num =  (0xff & _bytes[idx]).ToString("X");
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
			WritableComparator.Define(typeof(BytesWritable), new Comparator());
		}

	    
	    void IWritable.ReadFields(BinaryReader reader)
	    {
	        ReadFields(reader);
	    }
	}
}
