using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// A
	/// <see cref="GSet{K, E}"/>
	/// is set,
	/// which supports the
	/// <see cref="GSet{K, E}.Get(object)"/>
	/// operation.
	/// The
	/// <see cref="GSet{K, E}.Get(object)"/>
	/// operation uses a key to lookup an element.
	/// Null element is not supported.
	/// </summary>
	/// <?/>
	/// <?/>
	public abstract class GSet<K, E> : IEnumerable<E>
		where E : K
	{
		public const Log Log = LogFactory.GetLog(typeof(GSet));

		/// <returns>The size of this set.</returns>
		public abstract int Size();

		/// <summary>Does this set contain an element corresponding to the given key?</summary>
		/// <param name="key">The given key.</param>
		/// <returns>
		/// true if the given key equals to a stored element.
		/// Otherwise, return false.
		/// </returns>
		/// <exception cref="System.ArgumentNullException">if key == null.</exception>
		public abstract bool Contains(K key);

		/// <summary>Return the stored element which is equal to the given key.</summary>
		/// <remarks>
		/// Return the stored element which is equal to the given key.
		/// This operation is similar to
		/// <see cref="System.Collections.IDictionary{K, V}.Get(object)"/>
		/// .
		/// </remarks>
		/// <param name="key">The given key.</param>
		/// <returns>
		/// The stored element if it exists.
		/// Otherwise, return null.
		/// </returns>
		/// <exception cref="System.ArgumentNullException">if key == null.</exception>
		public abstract E Get(K key);

		/// <summary>Add/replace an element.</summary>
		/// <remarks>
		/// Add/replace an element.
		/// If the element does not exist, add it to the set.
		/// Otherwise, replace the existing element.
		/// Note that this operation
		/// is similar to
		/// <see cref="System.Collections.IDictionary{K, V}.Put(object, object)"/>
		/// but is different from
		/// <see cref="Sharpen.Set{E}.AddItem(object)"/>
		/// which does not replace the existing element if there is any.
		/// </remarks>
		/// <param name="element">The element being put.</param>
		/// <returns>
		/// the previous stored element if there is any.
		/// Otherwise, return null.
		/// </returns>
		/// <exception cref="System.ArgumentNullException">if element == null.</exception>
		public abstract E Put(E element);

		/// <summary>Remove the element corresponding to the given key.</summary>
		/// <remarks>
		/// Remove the element corresponding to the given key.
		/// This operation is similar to
		/// <see cref="Sharpen.Collections.Remove(object)"/>
		/// .
		/// </remarks>
		/// <param name="key">The key of the element being removed.</param>
		/// <returns>
		/// If such element exists, return it.
		/// Otherwise, return null.
		/// </returns>
		/// <exception cref="System.ArgumentNullException">if key == null.</exception>
		public abstract E Remove(K key);

		public abstract void Clear();
	}

	public static class GSetConstants
	{
	}
}
