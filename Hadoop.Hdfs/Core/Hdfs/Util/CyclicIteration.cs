using System;
using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>
	/// Provide an cyclic
	/// <see cref="System.Collections.IEnumerator{E}"/>
	/// for a
	/// <see cref="Sharpen.NavigableMap{K, V}"/>
	/// .
	/// The
	/// <see cref="System.Collections.IEnumerator{E}"/>
	/// navigates the entries of the map
	/// according to the map's ordering.
	/// If the
	/// <see cref="System.Collections.IEnumerator{E}"/>
	/// hits the last entry of the map,
	/// it will then continue from the first entry.
	/// </summary>
	public class CyclicIteration<K, V> : IEnumerable<KeyValuePair<K, V>>
	{
		private readonly NavigableMap<K, V> navigablemap;

		private readonly NavigableMap<K, V> tailmap;

		/// <summary>
		/// Construct an
		/// <see cref="System.Collections.IEnumerable{T}"/>
		/// object,
		/// so that an
		/// <see cref="System.Collections.IEnumerator{E}"/>
		/// can be created
		/// for iterating the given
		/// <see cref="Sharpen.NavigableMap{K, V}"/>
		/// .
		/// The iteration begins from the starting key exclusively.
		/// </summary>
		public CyclicIteration(NavigableMap<K, V> navigablemap, K startingkey)
		{
			if (navigablemap == null || navigablemap.IsEmpty())
			{
				this.navigablemap = null;
				this.tailmap = null;
			}
			else
			{
				this.navigablemap = navigablemap;
				this.tailmap = navigablemap.TailMap(startingkey, false);
			}
		}

		public override IEnumerator<KeyValuePair<K, V>> GetEnumerator()
		{
			return new CyclicIteration.CyclicIterator(this);
		}

		/// <summary>
		/// An
		/// <see cref="System.Collections.IEnumerator{E}"/>
		/// for
		/// <see cref="CyclicIteration{K, V}"/>
		/// .
		/// </summary>
		private class CyclicIterator : IEnumerator<KeyValuePair<K, V>>
		{
			private bool hasnext;

			private IEnumerator<KeyValuePair<K, V>> i;

			/// <summary>The first entry to begin.</summary>
			private readonly KeyValuePair<K, V> first;

			/// <summary>The next entry.</summary>
			private KeyValuePair<K, V> next;

			private CyclicIterator(CyclicIteration<K, V> _enclosing)
			{
				this._enclosing = _enclosing;
				this.hasnext = this._enclosing.navigablemap != null;
				if (this.hasnext)
				{
					this.i = this._enclosing.tailmap.GetEnumerator();
					this.first = this.NextEntry();
					this.next = this.first;
				}
				else
				{
					this.i = null;
					this.first = null;
					this.next = null;
				}
			}

			private KeyValuePair<K, V> NextEntry()
			{
				if (!this.i.HasNext())
				{
					this.i = this._enclosing.navigablemap.GetEnumerator();
				}
				return this.i.Next();
			}

			public override bool HasNext()
			{
				return this.hasnext;
			}

			public override KeyValuePair<K, V> Next()
			{
				if (!this.hasnext)
				{
					throw new NoSuchElementException();
				}
				KeyValuePair<K, V> curr = this.next;
				this.next = this.NextEntry();
				this.hasnext = !this.next.Equals(this.first);
				return curr;
			}

			/// <summary>Not supported</summary>
			public override void Remove()
			{
				throw new NotSupportedException("Not supported");
			}

			private readonly CyclicIteration<K, V> _enclosing;
		}
	}
}
