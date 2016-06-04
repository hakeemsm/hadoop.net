using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>Singleton Writable with no data.</summary>
	public class NullWritable : WritableComparable<Org.Apache.Hadoop.IO.NullWritable>
	{
		private static readonly Org.Apache.Hadoop.IO.NullWritable This = new Org.Apache.Hadoop.IO.NullWritable
			();

		private NullWritable()
		{
		}

		// no public ctor
		/// <summary>Returns the single instance of this class.</summary>
		public static Org.Apache.Hadoop.IO.NullWritable Get()
		{
			return This;
		}

		public override string ToString()
		{
			return "(null)";
		}

		public override int GetHashCode()
		{
			return 0;
		}

		public virtual int CompareTo(Org.Apache.Hadoop.IO.NullWritable other)
		{
			return 0;
		}

		public override bool Equals(object other)
		{
			return other is Org.Apache.Hadoop.IO.NullWritable;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
		}

		/// <summary>A Comparator &quot;optimized&quot; for NullWritable.</summary>
		public class Comparator : WritableComparator
		{
			public Comparator()
				: base(typeof(NullWritable))
			{
			}

			/// <summary>Compare the buffers in serialized form.</summary>
			public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				System.Diagnostics.Debug.Assert(0 == l1);
				System.Diagnostics.Debug.Assert(0 == l2);
				return 0;
			}
		}

		static NullWritable()
		{
			// register this comparator
			WritableComparator.Define(typeof(NullWritable), new NullWritable.Comparator());
		}
	}
}
