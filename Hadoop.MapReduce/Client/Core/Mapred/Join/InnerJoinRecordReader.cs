using System;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	/// <summary>Full inner join.</summary>
	public class InnerJoinRecordReader<K> : JoinRecordReader<K>
		where K : WritableComparable
	{
		/// <exception cref="System.IO.IOException"/>
		internal InnerJoinRecordReader(int id, JobConf conf, int capacity, Type cmpcl)
			: base(id, conf, capacity, cmpcl)
		{
		}

		/// <summary>Return true iff the tuple is full (all data sources contain this key).</summary>
		protected internal override bool Combine(object[] srcs, TupleWritable dst)
		{
			System.Diagnostics.Debug.Assert(srcs.Length == dst.Size());
			for (int i = 0; i < srcs.Length; ++i)
			{
				if (!dst.Has(i))
				{
					return false;
				}
			}
			return true;
		}
	}
}
