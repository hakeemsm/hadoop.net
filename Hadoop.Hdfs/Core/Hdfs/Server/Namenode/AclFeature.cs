using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Feature that represents the ACLs of the inode.</summary>
	public class AclFeature : INode.Feature, ReferenceCountMap.ReferenceCounter
	{
		public static readonly ImmutableList<AclEntry> EmptyEntryList = ImmutableList.Of(
			);

		private int refCount = 0;

		private readonly int[] entries;

		public AclFeature(int[] entries)
		{
			this.entries = entries;
		}

		/// <summary>Get the number of entries present</summary>
		internal virtual int GetEntriesSize()
		{
			return entries.Length;
		}

		/// <summary>Get the entry at the specified position</summary>
		/// <param name="pos">Position of the entry to be obtained</param>
		/// <returns>integer representation of AclEntry</returns>
		/// <exception cref="System.IndexOutOfRangeException">if pos out of bound</exception>
		internal virtual int GetEntryAt(int pos)
		{
			Preconditions.CheckPositionIndex(pos, entries.Length, "Invalid position for AclEntry"
				);
			return entries[pos];
		}

		public override bool Equals(object o)
		{
			if (o == null)
			{
				return false;
			}
			if (GetType() != o.GetType())
			{
				return false;
			}
			return Arrays.Equals(entries, ((Org.Apache.Hadoop.Hdfs.Server.Namenode.AclFeature
				)o).entries);
		}

		public override int GetHashCode()
		{
			return Arrays.HashCode(entries);
		}

		public virtual int GetRefCount()
		{
			return refCount;
		}

		public virtual int IncrementAndGetRefCount()
		{
			return ++refCount;
		}

		public virtual int DecrementAndGetRefCount()
		{
			return (refCount > 0) ? --refCount : 0;
		}
	}
}
