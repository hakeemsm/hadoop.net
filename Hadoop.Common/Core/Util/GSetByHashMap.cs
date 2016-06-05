using System;
using System.Collections.Generic;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// A
	/// <see cref="GSet{K, E}"/>
	/// implementation by
	/// <see cref="System.Collections.Hashtable{K, V}"/>
	/// .
	/// </summary>
	public class GSetByHashMap<K, E> : GSet<K, E>
		where E : K
	{
		private readonly Dictionary<K, E> m;

		public GSetByHashMap(int initialCapacity, float loadFactor)
		{
			m = new Dictionary<K, E>(initialCapacity, loadFactor);
		}

		public override int Size()
		{
			return m.Count;
		}

		public override bool Contains(K k)
		{
			return m.Contains(k);
		}

		public override E Get(K k)
		{
			return m[k];
		}

		public override E Put(E element)
		{
			if (element == null)
			{
				throw new NotSupportedException("Null element is not supported.");
			}
			return m[element] = element;
		}

		public override E Remove(K k)
		{
			return Collections.Remove(m, k);
		}

		public virtual IEnumerator<E> GetEnumerator()
		{
			return m.Values.GetEnumerator();
		}

		public override void Clear()
		{
			m.Clear();
		}
	}
}
