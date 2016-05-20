using Sharpen;

namespace org.apache.hadoop.util.bloom
{
	/// <summary>The general behavior of a key that must be stored in a filter.</summary>
	/// <seealso cref="Filter">The general behavior of a filter</seealso>
	public class Key : org.apache.hadoop.io.WritableComparable<org.apache.hadoop.util.bloom.Key
		>
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
			set(value, weight);
		}

		/// <param name="value"/>
		/// <param name="weight"/>
		public virtual void set(byte[] value, double weight)
		{
			if (value == null)
			{
				throw new System.ArgumentException("value can not be null");
			}
			this.bytes = value;
			this.weight = weight;
		}

		/// <returns>byte[] The value of <i>this</i> key.</returns>
		public virtual byte[] getBytes()
		{
			return this.bytes;
		}

		/// <returns>Returns the weight associated to <i>this</i> key.</returns>
		public virtual double getWeight()
		{
			return weight;
		}

		/// <summary>Increments the weight of <i>this</i> key with a specified value.</summary>
		/// <param name="weight">The increment.</param>
		public virtual void incrementWeight(double weight)
		{
			this.weight += weight;
		}

		/// <summary>Increments the weight of <i>this</i> key by one.</summary>
		public virtual void incrementWeight()
		{
			this.weight++;
		}

		public override bool Equals(object o)
		{
			if (!(o is org.apache.hadoop.util.bloom.Key))
			{
				return false;
			}
			return this.compareTo((org.apache.hadoop.util.bloom.Key)o) == 0;
		}

		public override int GetHashCode()
		{
			int result = 0;
			for (int i = 0; i < bytes.Length; i++)
			{
				result ^= byte.valueOf(bytes[i]).GetHashCode();
			}
			result ^= double.valueOf(weight).GetHashCode();
			return result;
		}

		// Writable
		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeInt(bytes.Length);
			@out.write(bytes);
			@out.writeDouble(weight);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			this.bytes = new byte[@in.readInt()];
			@in.readFully(this.bytes);
			weight = @in.readDouble();
		}

		// Comparable
		public virtual int compareTo(org.apache.hadoop.util.bloom.Key other)
		{
			int result = this.bytes.Length - other.getBytes().Length;
			for (int i = 0; result == 0 && i < bytes.Length; i++)
			{
				result = this.bytes[i] - other.bytes[i];
			}
			if (result == 0)
			{
				result = double.valueOf(this.weight - other.weight);
			}
			return result;
		}
	}
}
