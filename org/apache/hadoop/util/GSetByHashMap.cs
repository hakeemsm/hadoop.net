using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>
	/// A
	/// <see cref="GSet{K, E}"/>
	/// implementation by
	/// <see cref="System.Collections.Hashtable{K, V}"/>
	/// .
	/// </summary>
	public class GSetByHashMap<K, E> : org.apache.hadoop.util.GSet<K, E>
		where E : K
	{
		private readonly System.Collections.Generic.Dictionary<K, E> m;

		public GSetByHashMap(int initialCapacity, float loadFactor)
		{
			m = new System.Collections.Generic.Dictionary<K, E>(initialCapacity, loadFactor);
		}

		public override int size()
		{
			return m.Count;
		}

		public override bool contains(K k)
		{
			return m.Contains(k);
		}

		public override E get(K k)
		{
			return m[k];
		}

		public override E put(E element)
		{
			if (element == null)
			{
				throw new System.NotSupportedException("Null element is not supported.");
			}
			return m[element] = element;
		}

		public override E remove(K k)
		{
			return Sharpen.Collections.Remove(m, k);
		}

		public virtual System.Collections.Generic.IEnumerator<E> GetEnumerator()
		{
			return m.Values.GetEnumerator();
		}

		public override void clear()
		{
			m.clear();
		}
	}
}
