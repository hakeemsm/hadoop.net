using System;
using System.IO;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Util.Bloom
{
	/// <summary>The general behavior of a key that must be stored in a filter.</summary>
	/// <seealso cref="Filter">The general behavior of a filter</seealso>
	public class Key : IWritableComparable<Org.Apache.Hadoop.Util.Bloom.Key>
	{
		/// <summary>Byte value of key</summary>
		internal byte[] bytes;

		/// <summary>The weight associated to <i>this</i> key.</summary>
		/// <remarks>
		/// The weight associated to <i>this</i> key.
		/// <p>
		/// <b>Invariant</b>: if it is not specified, each instance of
		/// <code>Key</code> will have a default weight of 1.0
		/// </remarks>
		internal double weight;

		/// <summary>default constructor - use with readFields</summary>
		public Key()
		{
		}

		/// <summary>Constructor.</summary>
		/// <remarks>
		/// Constructor.
		/// <p>
		/// Builds a key with a default weight.
		/// </remarks>
		/// <param name="value">The byte value of <i>this</i> key.</param>
		public Key(byte[] value)
			: this(value, 1.0)
		{
		}

		/// <summary>Constructor.</summary>
		/// <remarks>
		/// Constructor.
		/// <p>
		/// Builds a key with a specified weight.
		/// </remarks>
		/// <param name="value">The value of <i>this</i> key.</param>
		/// <param name="weight">The weight associated to <i>this</i> key.</param>
		public Key(byte[] value, double weight)
		{
			Set(value, weight);
		}

		/// <param name="value"/>
		/// <param name="weight"/>
		public virtual void Set(byte[] value, double weight)
		{
			if (value == null)
			{
				throw new ArgumentException("value can not be null");
			}
			this.bytes = value;
			this.weight = weight;
		}

		/// <returns>byte[] The value of <i>this</i> key.</returns>
		public virtual byte[] GetBytes()
		{
			return this.bytes;
		}

		/// <returns>Returns the weight associated to <i>this</i> key.</returns>
		public virtual double GetWeight()
		{
			return weight;
		}

		/// <summary>Increments the weight of <i>this</i> key with a specified value.</summary>
		/// <param name="weight">The increment.</param>
		public virtual void IncrementWeight(double weight)
		{
			this.weight += weight;
		}

		/// <summary>Increments the weight of <i>this</i> key by one.</summary>
		public virtual void IncrementWeight()
		{
			this.weight++;
		}

		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.Util.Bloom.Key))
			{
				return false;
			}
			return this.CompareTo((Org.Apache.Hadoop.Util.Bloom.Key)o) == 0;
		}

		public override int GetHashCode()
		{
			int result = 0;
			for (int i = 0; i < bytes.Length; i++)
			{
				result ^= byte.ValueOf(bytes[i]).GetHashCode();
			}
			result ^= double.ValueOf(weight).GetHashCode();
			return result;
		}

		// Writable
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteInt(bytes.Length);
			@out.Write(bytes);
			@out.WriteDouble(weight);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			this.bytes = new byte[@in.ReadInt()];
			@in.ReadFully(this.bytes);
			weight = @in.ReadDouble();
		}

		// Comparable
		public virtual int CompareTo(Org.Apache.Hadoop.Util.Bloom.Key other)
		{
			int result = this.bytes.Length - other.GetBytes().Length;
			for (int i = 0; result == 0 && i < bytes.Length; i++)
			{
				result = this.bytes[i] - other.bytes[i];
			}
			if (result == 0)
			{
				result = double.ValueOf(this.weight - other.weight);
			}
			return result;
		}
	}
}
