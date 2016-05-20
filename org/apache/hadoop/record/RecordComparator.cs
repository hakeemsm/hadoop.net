using Sharpen;

namespace org.apache.hadoop.record
{
	/// <summary>A raw record comparator base class</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public abstract class RecordComparator : org.apache.hadoop.io.WritableComparator
	{
		/// <summary>
		/// Construct a raw
		/// <see cref="Record"/>
		/// comparison implementation.
		/// </summary>
		protected internal RecordComparator(java.lang.Class recordClass)
			: base(recordClass)
		{
		}

		// inheric JavaDoc
		public abstract override int compare(byte[] b1, int s1, int l1, byte[] b2, int s2
			, int l2);

		/// <summary>
		/// Register an optimized comparator for a
		/// <see cref="Record"/>
		/// implementation.
		/// </summary>
		/// <param name="c">record classs for which a raw comparator is provided</param>
		/// <param name="comparator">Raw comparator instance for class c</param>
		public static void define(java.lang.Class c, org.apache.hadoop.record.RecordComparator
			 comparator)
		{
			lock (typeof(RecordComparator))
			{
				org.apache.hadoop.io.WritableComparator.define(c, comparator);
			}
		}
	}
}
