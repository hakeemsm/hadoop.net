using System;
using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.Record
{
	/// <summary>A raw record comparator base class</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public abstract class RecordComparator : WritableComparator
	{
		/// <summary>
		/// Construct a raw
		/// <see cref="Record"/>
		/// comparison implementation.
		/// </summary>
		protected internal RecordComparator(Type recordClass)
			: base(recordClass)
		{
		}

		// inheric JavaDoc
		public abstract override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2
			, int l2);

		/// <summary>
		/// Register an optimized comparator for a
		/// <see cref="Record"/>
		/// implementation.
		/// </summary>
		/// <param name="c">record classs for which a raw comparator is provided</param>
		/// <param name="comparator">Raw comparator instance for class c</param>
		public static void Define(Type c, Org.Apache.Hadoop.Record.RecordComparator comparator
			)
		{
			lock (typeof(RecordComparator))
			{
				WritableComparator.Define(c, comparator);
			}
		}
	}
}
