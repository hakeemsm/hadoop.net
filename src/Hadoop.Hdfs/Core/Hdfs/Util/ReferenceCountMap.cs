using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Com.Google.Common.Collect;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>Class for de-duplication of instances.</summary>
	/// <remarks>
	/// Class for de-duplication of instances. <br />
	/// Hold the references count to a single instance. If there are no references
	/// then the entry will be removed.<br />
	/// Type E should implement
	/// <see cref="ReferenceCounter"/>
	/// <br />
	/// Note: This class is NOT thread-safe.
	/// </remarks>
	public class ReferenceCountMap<E>
		where E : ReferenceCountMap.ReferenceCounter
	{
		private IDictionary<E, E> referenceMap = new Dictionary<E, E>();

		/// <summary>Add the reference.</summary>
		/// <remarks>
		/// Add the reference. If the instance already present, just increase the
		/// reference count.
		/// </remarks>
		/// <param name="key">Key to put in reference map</param>
		/// <returns>Referenced instance</returns>
		public virtual E Put(E key)
		{
			E value = referenceMap[key];
			if (value == null)
			{
				value = key;
				referenceMap[key] = value;
			}
			value.IncrementAndGetRefCount();
			return value;
		}

		/// <summary>Delete the reference.</summary>
		/// <remarks>
		/// Delete the reference. Decrease the reference count for the instance, if
		/// any. On all references removal delete the instance from the map.
		/// </remarks>
		/// <param name="key">Key to remove the reference.</param>
		public virtual void Remove(E key)
		{
			E value = referenceMap[key];
			if (value != null && value.DecrementAndGetRefCount() == 0)
			{
				Sharpen.Collections.Remove(referenceMap, key);
			}
		}

		/// <summary>Get entries in the reference Map.</summary>
		/// <returns/>
		[VisibleForTesting]
		public virtual ImmutableList<E> GetEntries()
		{
			return ((ImmutableList<E>)((ImmutableList.Builder<E>)new ImmutableList.Builder<E>
				().AddAll(referenceMap.Keys)).Build());
		}

		/// <summary>Get the reference count for the key</summary>
		public virtual long GetReferenceCount(E key)
		{
			ReferenceCountMap.ReferenceCounter counter = referenceMap[key];
			if (counter != null)
			{
				return counter.GetRefCount();
			}
			return 0;
		}

		/// <summary>Get the number of unique elements</summary>
		public virtual int GetUniqueElementsSize()
		{
			return referenceMap.Count;
		}

		/// <summary>Clear the contents</summary>
		[VisibleForTesting]
		public virtual void Clear()
		{
			referenceMap.Clear();
		}

		/// <summary>Interface for the reference count holder</summary>
		public interface ReferenceCounter
		{
			int GetRefCount();

			int IncrementAndGetRefCount();

			int DecrementAndGetRefCount();
		}
	}
}
