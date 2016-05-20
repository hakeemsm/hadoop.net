using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Singleton Writable with no data.</summary>
	public class NullWritable : org.apache.hadoop.io.WritableComparable<org.apache.hadoop.io.NullWritable
		>
	{
		private static readonly org.apache.hadoop.io.NullWritable THIS = new org.apache.hadoop.io.NullWritable
			();

		private NullWritable()
		{
		}

		// no public ctor
		/// <summary>Returns the single instance of this class.</summary>
		public static org.apache.hadoop.io.NullWritable get()
		{
			return THIS;
		}

		public override string ToString()
		{
			return "(null)";
		}

		public override int GetHashCode()
		{
			return 0;
		}

		public virtual int compareTo(org.apache.hadoop.io.NullWritable other)
		{
			return 0;
		}

		public override bool Equals(object other)
		{
			return other is org.apache.hadoop.io.NullWritable;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
		}

		/// <summary>A Comparator &quot;optimized&quot; for NullWritable.</summary>
		public class Comparator : org.apache.hadoop.io.WritableComparator
		{
			public Comparator()
				: base(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.NullWritable))
					)
			{
			}

			/// <summary>Compare the buffers in serialized form.</summary>
			public override int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				System.Diagnostics.Debug.Assert(0 == l1);
				System.Diagnostics.Debug.Assert(0 == l2);
				return 0;
			}
		}

		static NullWritable()
		{
			// register this comparator
			org.apache.hadoop.io.WritableComparator.define(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.NullWritable)), new org.apache.hadoop.io.NullWritable.Comparator
				());
		}
	}
}
