using System;
using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// <code>MarkableIterator</code> is a wrapper iterator class that
	/// implements the
	/// <see cref="MarkableIteratorInterface{VALUE}"/>
	/// .
	/// </summary>
	public class MarkableIterator<Value> : MarkableIteratorInterface<VALUE>
	{
		internal MarkableIteratorInterface<VALUE> baseIterator;

		/// <summary>Create a new iterator layered on the input iterator</summary>
		/// <param name="itr">underlying iterator that implements MarkableIteratorInterface</param>
		public MarkableIterator(IEnumerator<VALUE> itr)
		{
			if (!(itr is MarkableIteratorInterface))
			{
				throw new ArgumentException("Input Iterator not markable");
			}
			baseIterator = (MarkableIteratorInterface<VALUE>)itr;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Mark()
		{
			baseIterator.Mark();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Reset()
		{
			baseIterator.Reset();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ClearMark()
		{
			baseIterator.ClearMark();
		}

		public virtual bool HasNext()
		{
			return baseIterator.HasNext();
		}

		public virtual VALUE Next()
		{
			return baseIterator.Next();
		}

		public virtual void Remove()
		{
			throw new NotSupportedException("Remove Not Implemented");
		}
	}
}
