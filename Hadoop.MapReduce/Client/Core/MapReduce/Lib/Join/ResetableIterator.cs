using System;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Join
{
	/// <summary>
	/// This defines an interface to a stateful Iterator that can replay elements
	/// added to it directly.
	/// </summary>
	/// <remarks>
	/// This defines an interface to a stateful Iterator that can replay elements
	/// added to it directly.
	/// Note that this does not extend
	/// <see cref="System.Collections.IEnumerator{E}"/>
	/// .
	/// </remarks>
	public abstract class ResetableIterator<T>
		where T : Writable
	{
		public class EMPTY<U> : ResetableIterator<U>
			where U : Writable
		{
			public override bool HasNext()
			{
				return false;
			}

			public override void Reset()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
			}

			public override void Clear()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Next(U val)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Replay(U val)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Add(U item)
			{
				throw new NotSupportedException();
			}
		}

		/// <summary>True if a call to next may return a value.</summary>
		/// <remarks>
		/// True if a call to next may return a value. This is permitted false
		/// positives, but not false negatives.
		/// </remarks>
		public abstract bool HasNext();

		/// <summary>Assign next value to actual.</summary>
		/// <remarks>
		/// Assign next value to actual.
		/// It is required that elements added to a ResetableIterator be returned in
		/// the same order after a call to
		/// <see cref="ResetableIterator{T}.Reset()"/>
		/// (FIFO).
		/// Note that a call to this may fail for nested joins (i.e. more elements
		/// available, but none satisfying the constraints of the join)
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool Next(T val);

		/// <summary>Assign last value returned to actual.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool Replay(T val);

		/// <summary>Set iterator to return to the start of its range.</summary>
		/// <remarks>
		/// Set iterator to return to the start of its range. Must be called after
		/// calling
		/// <see cref="ResetableIterator{T}.Add(Org.Apache.Hadoop.IO.Writable)"/>
		/// to avoid a ConcurrentModificationException.
		/// </remarks>
		public abstract void Reset();

		/// <summary>Add an element to the collection of elements to iterate over.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Add(T item);

		/// <summary>Close datasources and release resources.</summary>
		/// <remarks>
		/// Close datasources and release resources. Calling methods on the iterator
		/// after calling close has undefined behavior.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Close();

		// XXX is this necessary?
		/// <summary>Close datasources, but do not release internal resources.</summary>
		/// <remarks>
		/// Close datasources, but do not release internal resources. Calling this
		/// method should permit the object to be reused with a different datasource.
		/// </remarks>
		public abstract void Clear();
	}

	public static class ResetableIteratorConstants
	{
	}
}
