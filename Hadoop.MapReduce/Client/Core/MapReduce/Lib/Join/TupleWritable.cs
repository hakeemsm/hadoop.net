using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Join
{
	/// <summary>
	/// Writable type storing multiple
	/// <see cref="Org.Apache.Hadoop.IO.Writable"/>
	/// s.
	/// This is *not* a general-purpose tuple type. In almost all cases, users are
	/// encouraged to implement their own serializable types, which can perform
	/// better validation and provide more efficient encodings than this class is
	/// capable. TupleWritable relies on the join framework for type safety and
	/// assumes its instances will rarely be persisted, assumptions not only
	/// incompatible with, but contrary to the general case.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.IO.Writable"/>
	public class TupleWritable : Writable, IEnumerable<Writable>
	{
		protected internal BitSet written;

		private Writable[] values;

		/// <summary>Create an empty tuple with no allocated storage for writables.</summary>
		public TupleWritable()
		{
			written = new BitSet(0);
		}

		/// <summary>
		/// Initialize tuple with storage; unknown whether any of them contain
		/// &quot;written&quot; values.
		/// </summary>
		public TupleWritable(Writable[] vals)
		{
			written = new BitSet(vals.Length);
			values = vals;
		}

		/// <summary>Return true if tuple has an element at the position provided.</summary>
		public virtual bool Has(int i)
		{
			return written.Get(i);
		}

		/// <summary>Get ith Writable from Tuple.</summary>
		public virtual Writable Get(int i)
		{
			return values[i];
		}

		/// <summary>The number of children in this Tuple.</summary>
		public virtual int Size()
		{
			return values.Length;
		}

		/// <summary><inheritDoc/></summary>
		public override bool Equals(object other)
		{
			if (other is Org.Apache.Hadoop.Mapreduce.Lib.Join.TupleWritable)
			{
				Org.Apache.Hadoop.Mapreduce.Lib.Join.TupleWritable that = (Org.Apache.Hadoop.Mapreduce.Lib.Join.TupleWritable
					)other;
				if (!this.written.Equals(that.written))
				{
					return false;
				}
				for (int i = 0; i < values.Length; ++i)
				{
					if (!Has(i))
					{
						continue;
					}
					if (!values[i].Equals(that.Get(i)))
					{
						return false;
					}
				}
				return true;
			}
			return false;
		}

		public override int GetHashCode()
		{
			System.Diagnostics.Debug.Assert(false, "hashCode not designed");
			return written.GetHashCode();
		}

		/// <summary>Return an iterator over the elements in this tuple.</summary>
		/// <remarks>
		/// Return an iterator over the elements in this tuple.
		/// Note that this doesn't flatten the tuple; one may receive tuples
		/// from this iterator.
		/// </remarks>
		public virtual IEnumerator<Writable> GetEnumerator()
		{
			Org.Apache.Hadoop.Mapreduce.Lib.Join.TupleWritable t = this;
			return new _IEnumerator_123(this, t);
		}

		private sealed class _IEnumerator_123 : IEnumerator<Writable>
		{
			public _IEnumerator_123(TupleWritable _enclosing, Org.Apache.Hadoop.Mapreduce.Lib.Join.TupleWritable
				 t)
			{
				this._enclosing = _enclosing;
				this.t = t;
				this.bitIndex = this._enclosing.written.NextSetBit(0);
			}

			internal int bitIndex;

			public override bool HasNext()
			{
				return this.bitIndex >= 0;
			}

			public override Writable Next()
			{
				int returnIndex = this.bitIndex;
				if (returnIndex < 0)
				{
					throw new NoSuchElementException();
				}
				this.bitIndex = this._enclosing.written.NextSetBit(this.bitIndex + 1);
				return t.Get(returnIndex);
			}

			public override void Remove()
			{
				if (!this._enclosing.written.Get(this.bitIndex))
				{
					throw new InvalidOperationException("Attempt to remove non-existent val");
				}
				this._enclosing.written.Clear(this.bitIndex);
			}

			private readonly TupleWritable _enclosing;

			private readonly Org.Apache.Hadoop.Mapreduce.Lib.Join.TupleWritable t;
		}

		/// <summary>Convert Tuple to String as in the following.</summary>
		/// <remarks>
		/// Convert Tuple to String as in the following.
		/// <tt>[&lt;child1&gt;,&lt;child2&gt;,...,&lt;childn&gt;]</tt>
		/// </remarks>
		public override string ToString()
		{
			StringBuilder buf = new StringBuilder("[");
			for (int i = 0; i < values.Length; ++i)
			{
				buf.Append(Has(i) ? values[i].ToString() : string.Empty);
				buf.Append(",");
			}
			if (values.Length != 0)
			{
				Sharpen.Runtime.SetCharAt(buf, buf.Length - 1, ']');
			}
			else
			{
				buf.Append(']');
			}
			return buf.ToString();
		}

		// Writable
		/// <summary>Writes each Writable to <code>out</code>.</summary>
		/// <remarks>
		/// Writes each Writable to <code>out</code>.
		/// TupleWritable format:
		/// <c>&lt;count&gt;&lt;type1&gt;&lt;type2&gt;...&lt;typen&gt;&lt;obj1&gt;&lt;obj2&gt;...&lt;objn&gt;
		/// 	</c>
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			WritableUtils.WriteVInt(@out, values.Length);
			WriteBitSet(@out, values.Length, written);
			for (int i = 0; i < values.Length; ++i)
			{
				Org.Apache.Hadoop.IO.Text.WriteString(@out, values[i].GetType().FullName);
			}
			for (int i_1 = 0; i_1 < values.Length; ++i_1)
			{
				if (Has(i_1))
				{
					values[i_1].Write(@out);
				}
			}
		}

		/// <summary><inheritDoc/></summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			// No static typeinfo on Tuples
			int card = WritableUtils.ReadVInt(@in);
			values = new Writable[card];
			ReadBitSet(@in, card, written);
			Type[] cls = new Type[card];
			try
			{
				for (int i = 0; i < card; ++i)
				{
					cls[i] = Sharpen.Runtime.GetType(Org.Apache.Hadoop.IO.Text.ReadString(@in)).AsSubclass
						<Writable>();
				}
				for (int i_1 = 0; i_1 < card; ++i_1)
				{
					if (cls[i_1].Equals(typeof(NullWritable)))
					{
						values[i_1] = NullWritable.Get();
					}
					else
					{
						values[i_1] = System.Activator.CreateInstance(cls[i_1]);
					}
					if (Has(i_1))
					{
						values[i_1].ReadFields(@in);
					}
				}
			}
			catch (TypeLoadException e)
			{
				throw new IOException("Failed tuple init", e);
			}
			catch (MemberAccessException e)
			{
				throw new IOException("Failed tuple init", e);
			}
			catch (InstantiationException e)
			{
				throw new IOException("Failed tuple init", e);
			}
		}

		/// <summary>Record that the tuple contains an element at the position provided.</summary>
		internal virtual void SetWritten(int i)
		{
			written.Set(i);
		}

		/// <summary>
		/// Record that the tuple does not contain an element at the position
		/// provided.
		/// </summary>
		internal virtual void ClearWritten(int i)
		{
			written.Clear(i);
		}

		/// <summary>
		/// Clear any record of which writables have been written to, without
		/// releasing storage.
		/// </summary>
		internal virtual void ClearWritten()
		{
			written.Clear();
		}

		/// <summary>Writes the bit set to the stream.</summary>
		/// <remarks>
		/// Writes the bit set to the stream. The first 64 bit-positions of the bit
		/// set are written as a VLong for backwards-compatibility with older
		/// versions of TupleWritable. All bit-positions &gt;= 64 are encoded as a byte
		/// for every 8 bit-positions.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private static void WriteBitSet(DataOutput stream, int nbits, BitSet bitSet)
		{
			long bits = 0L;
			int bitSetIndex = bitSet.NextSetBit(0);
			for (; bitSetIndex >= 0 && bitSetIndex < long.Size; bitSetIndex = bitSet.NextSetBit
				(bitSetIndex + 1))
			{
				bits |= 1L << bitSetIndex;
			}
			WritableUtils.WriteVLong(stream, bits);
			if (nbits > long.Size)
			{
				bits = 0L;
				for (int lastWordWritten = 0; bitSetIndex >= 0 && bitSetIndex < nbits; bitSetIndex
					 = bitSet.NextSetBit(bitSetIndex + 1))
				{
					int bitsIndex = bitSetIndex % byte.Size;
					int word = (bitSetIndex - long.Size) / byte.Size;
					if (word > lastWordWritten)
					{
						stream.WriteByte(unchecked((byte)bits));
						bits = 0L;
						for (lastWordWritten++; lastWordWritten < word; lastWordWritten++)
						{
							stream.WriteByte(unchecked((byte)bits));
						}
					}
					bits |= 1L << bitsIndex;
				}
				stream.WriteByte(unchecked((byte)bits));
			}
		}

		/// <summary>
		/// Reads a bitset from the stream that has been written with
		/// <see cref="WriteBitSet(System.IO.DataOutput, int, Sharpen.BitSet)"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static void ReadBitSet(DataInput stream, int nbits, BitSet bitSet)
		{
			bitSet.Clear();
			long initialBits = WritableUtils.ReadVLong(stream);
			long last = 0L;
			while (0L != initialBits)
			{
				last = long.LowestOneBit(initialBits);
				initialBits ^= last;
				bitSet.Set(long.NumberOfTrailingZeros(last));
			}
			for (int offset = long.Size; offset < nbits; offset += byte.Size)
			{
				byte bits = stream.ReadByte();
				while (0 != bits)
				{
					last = long.LowestOneBit(bits);
					bits ^= last;
					bitSet.Set(long.NumberOfTrailingZeros(last) + offset);
				}
			}
		}
	}
}
