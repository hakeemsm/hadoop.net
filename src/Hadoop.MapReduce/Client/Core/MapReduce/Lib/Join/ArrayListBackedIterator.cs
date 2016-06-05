using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Join
{
	/// <summary>This class provides an implementation of ResetableIterator.</summary>
	/// <remarks>
	/// This class provides an implementation of ResetableIterator. The
	/// implementation uses an
	/// <see cref="System.Collections.ArrayList{E}"/>
	/// to store elements
	/// added to it, replaying them as requested.
	/// Prefer
	/// <see cref="StreamBackedIterator{X}"/>
	/// .
	/// </remarks>
	public class ArrayListBackedIterator<X> : ResetableIterator<X>
		where X : Writable
	{
		private IEnumerator<X> iter;

		private AList<X> data;

		private X hold = null;

		private Configuration conf = new Configuration();

		public ArrayListBackedIterator()
			: this(new AList<X>())
		{
		}

		public ArrayListBackedIterator(AList<X> data)
		{
			this.data = data;
			this.iter = this.data.GetEnumerator();
		}

		public override bool HasNext()
		{
			return iter.HasNext();
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Next(X val)
		{
			if (iter.HasNext())
			{
				ReflectionUtils.Copy(conf, iter.Next(), val);
				if (null == hold)
				{
					hold = WritableUtils.Clone(val, null);
				}
				else
				{
					ReflectionUtils.Copy(conf, val, hold);
				}
				return true;
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Replay(X val)
		{
			ReflectionUtils.Copy(conf, hold, val);
			return true;
		}

		public override void Reset()
		{
			iter = data.GetEnumerator();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Add(X item)
		{
			data.AddItem(WritableUtils.Clone(item, null));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			iter = null;
			data = null;
		}

		public override void Clear()
		{
			data.Clear();
			Reset();
		}
	}
}
