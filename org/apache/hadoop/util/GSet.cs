using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>
	/// A
	/// <see cref="GSet{K, E}"/>
	/// is set,
	/// which supports the
	/// <see cref="GSet{K, E}.get(object)"/>
	/// operation.
	/// The
	/// <see cref="GSet{K, E}.get(object)"/>
	/// operation uses a key to lookup an element.
	/// Null element is not supported.
	/// </summary>
	/// <?/>
	/// <?/>
	public abstract class GSet<K, E> : System.Collections.Generic.IEnumerable<E>
		where E : K
	{
		public const org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.GSet)));

		/// <returns>The size of this set.</returns>
		public abstract int size();

		/// <summary>Does this set contain an element corresponding to the given key?</summary>
		/// <param name="key">The given key.</param>
		/// <returns>
		/// true if the given key equals to a stored element.
		/// Otherwise, return false.
		/// </returns>
		/// <exception cref="System.ArgumentNullException">if key == null.</exception>
		public abstract bool contains(K key);

		/// <summary>Return the stored element which is equal to the given key.</summary>
		/// <remarks>
		/// Return the stored element which is equal to the given key.
		/// This operation is similar to
		/// <see cref="System.Collections.IDictionary{K, V}.get(object)"/>
		/// .
		/// </remarks>
		/// <param name="key">The given key.</param>
		/// <returns>
		/// The stored element if it exists.
		/// Otherwise, return null.
		/// </returns>
		/// <exception cref="System.ArgumentNullException">if key == null.</exception>
		public abstract E get(K key);

		/// <summary>Add/replace an element.</summary>
		/// <remarks>
		/// Add/replace an element.
		/// If the element does not exist, add it to the set.
		/// Otherwise, replace the existing element.
		/// Note that this operation
		/// is similar to
		/// <see cref="System.Collections.IDictionary{K, V}.put(object, object)"/>
		/// but is different from
		/// <see cref="java.util.Set{E}.add(object)"/>
		/// which does not replace the existing element if there is any.
		/// </remarks>
		/// <param name="element">The element being put.</param>
		/// <returns>
		/// the previous stored element if there is any.
		/// Otherwise, return null.
		/// </returns>
		/// <exception cref="System.ArgumentNullException">if element == null.</exception>
		public abstract E put(E element);

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
		public abstract E remove(K key);

		public abstract void clear();
	}

	public static class GSetConstants
	{
	}
}
