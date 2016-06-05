using System;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	/// <summary>Prefer the &quot;rightmost&quot; data source for this key.</summary>
	/// <remarks>
	/// Prefer the &quot;rightmost&quot; data source for this key.
	/// For example, <tt>override(S1,S2,S3)</tt> will prefer values
	/// from S3 over S2, and values from S2 over S1 for all keys
	/// emitted from all sources.
	/// </remarks>
	public class OverrideRecordReader<K, V> : MultiFilterRecordReader<K, V>
		where K : WritableComparable
		where V : Writable
	{
		/// <exception cref="System.IO.IOException"/>
		internal OverrideRecordReader(int id, JobConf conf, int capacity, Type cmpcl)
			: base(id, conf, capacity, cmpcl)
		{
		}

		/// <summary>Emit the value with the highest position in the tuple.</summary>
		protected internal override V Emit(TupleWritable dst)
		{
			// No static typeinfo on Tuples
			return (V)dst.GetEnumerator().Next();
		}

		/// <summary>
		/// Instead of filling the JoinCollector with iterators from all
		/// data sources, fill only the rightmost for this key.
		/// </summary>
		/// <remarks>
		/// Instead of filling the JoinCollector with iterators from all
		/// data sources, fill only the rightmost for this key.
		/// This not only saves space by discarding the other sources, but
		/// it also emits the number of key-value pairs in the preferred
		/// RecordReader instead of repeating that stream n times, where
		/// n is the cardinality of the cross product of the discarded
		/// streams for the given key.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		protected internal override void FillJoinCollector(K iterkey)
		{
			PriorityQueue<ComposableRecordReader<K, object>> q = GetRecordReaderQueue();
			if (!q.IsEmpty())
			{
				int highpos = -1;
				AList<ComposableRecordReader<K, object>> list = new AList<ComposableRecordReader<
					K, object>>(kids.Length);
				q.Peek().Key(iterkey);
				WritableComparator cmp = GetComparator();
				while (0 == cmp.Compare(q.Peek().Key(), iterkey))
				{
					ComposableRecordReader<K, object> t = q.Poll();
					if (-1 == highpos || list[highpos].Id() < t.Id())
					{
						highpos = list.Count;
					}
					list.AddItem(t);
					if (q.IsEmpty())
					{
						break;
					}
				}
				ComposableRecordReader<K, object> t_1 = list.Remove(highpos);
				t_1.Accept(jc, iterkey);
				foreach (ComposableRecordReader<K, object> rr in list)
				{
					rr.Skip(iterkey);
				}
				list.AddItem(t_1);
				foreach (ComposableRecordReader<K, object> rr_1 in list)
				{
					if (rr_1.HasNext())
					{
						q.AddItem(rr_1);
					}
				}
			}
		}
	}
}
