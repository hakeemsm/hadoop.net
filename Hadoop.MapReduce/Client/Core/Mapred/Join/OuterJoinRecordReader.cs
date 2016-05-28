using System;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	/// <summary>Full outer join.</summary>
	public class OuterJoinRecordReader<K> : JoinRecordReader<K>
		where K : WritableComparable
	{
		/// <exception cref="System.IO.IOException"/>
		internal OuterJoinRecordReader(int id, JobConf conf, int capacity, Type cmpcl)
			: base(id, conf, capacity, cmpcl)
		{
		}

		/// <summary>Emit everything from the collector.</summary>
		protected internal override bool Combine(object[] srcs, TupleWritable dst)
		{
			System.Diagnostics.Debug.Assert(srcs.Length == dst.Size());
			return true;
		}
	}
}
