using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Join
{
	/// <summary>Full outer join.</summary>
	public class OuterJoinRecordReader<K> : JoinRecordReader<K>
		where K : WritableComparable<object>
	{
		/// <exception cref="System.IO.IOException"/>
		internal OuterJoinRecordReader(int id, Configuration conf, int capacity, Type cmpcl
			)
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
