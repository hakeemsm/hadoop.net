using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.FS;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// A block storage policy describes how to select the storage types
	/// for the replicas of a block.
	/// </summary>
	public class BlockStoragePolicy
	{
		public static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Protocol.BlockStoragePolicy
			));

		/// <summary>A 4-bit policy ID</summary>
		private readonly byte id;

		/// <summary>Policy name</summary>
		private readonly string name;

		/// <summary>The storage types to store the replicas of a new block.</summary>
		private readonly StorageType[] storageTypes;

		/// <summary>The fallback storage type for block creation.</summary>
		private readonly StorageType[] creationFallbacks;

		/// <summary>The fallback storage type for replication.</summary>
		private readonly StorageType[] replicationFallbacks;

		/// <summary>Whether the policy is inherited during file creation.</summary>
		/// <remarks>
		/// Whether the policy is inherited during file creation.
		/// If set then the policy cannot be changed after file creation.
		/// </remarks>
		private bool copyOnCreateFile;

		[VisibleForTesting]
		public BlockStoragePolicy(byte id, string name, StorageType[] storageTypes, StorageType
			[] creationFallbacks, StorageType[] replicationFallbacks)
			: this(id, name, storageTypes, creationFallbacks, replicationFallbacks, false)
		{
		}

		[VisibleForTesting]
		public BlockStoragePolicy(byte id, string name, StorageType[] storageTypes, StorageType
			[] creationFallbacks, StorageType[] replicationFallbacks, bool copyOnCreateFile)
		{
			this.id = id;
			this.name = name;
			this.storageTypes = storageTypes;
			this.creationFallbacks = creationFallbacks;
			this.replicationFallbacks = replicationFallbacks;
			this.copyOnCreateFile = copyOnCreateFile;
		}

		/// <returns>
		/// a list of
		/// <see cref="Org.Apache.Hadoop.FS.StorageType"/>
		/// s for storing the replicas of a block.
		/// </returns>
		public virtual IList<StorageType> ChooseStorageTypes(short replication)
		{
			IList<StorageType> types = new List<StorageType>();
			int i = 0;
			int j = 0;
			// Do not return transient storage types. We will not have accurate
			// usage information for transient types.
			for (; i < replication && j < storageTypes.Length; ++j)
			{
				if (!storageTypes[j].IsTransient())
				{
					types.AddItem(storageTypes[j]);
					++i;
				}
			}
			StorageType last = storageTypes[storageTypes.Length - 1];
			if (!last.IsTransient())
			{
				for (; i < replication; i++)
				{
					types.AddItem(last);
				}
			}
			return types;
		}

		/// <summary>
		/// Choose the storage types for storing the remaining replicas, given the
		/// replication number and the storage types of the chosen replicas.
		/// </summary>
		/// <param name="replication">the replication number.</param>
		/// <param name="chosen">the storage types of the chosen replicas.</param>
		/// <returns>
		/// a list of
		/// <see cref="Org.Apache.Hadoop.FS.StorageType"/>
		/// s for storing the replicas of a block.
		/// </returns>
		public virtual IList<StorageType> ChooseStorageTypes(short replication, IEnumerable
			<StorageType> chosen)
		{
			return ChooseStorageTypes(replication, chosen, null);
		}

		private IList<StorageType> ChooseStorageTypes(short replication, IEnumerable<StorageType
			> chosen, IList<StorageType> excess)
		{
			IList<StorageType> types = ChooseStorageTypes(replication);
			Diff(types, chosen, excess);
			return types;
		}

		/// <summary>
		/// Choose the storage types for storing the remaining replicas, given the
		/// replication number, the storage types of the chosen replicas and
		/// the unavailable storage types.
		/// </summary>
		/// <remarks>
		/// Choose the storage types for storing the remaining replicas, given the
		/// replication number, the storage types of the chosen replicas and
		/// the unavailable storage types. It uses fallback storage in case that
		/// the desired storage type is unavailable.
		/// </remarks>
		/// <param name="replication">the replication number.</param>
		/// <param name="chosen">the storage types of the chosen replicas.</param>
		/// <param name="unavailables">the unavailable storage types.</param>
		/// <param name="isNewBlock">Is it for new block creation?</param>
		/// <returns>
		/// a list of
		/// <see cref="Org.Apache.Hadoop.FS.StorageType"/>
		/// s for storing the replicas of a block.
		/// </returns>
		public virtual IList<StorageType> ChooseStorageTypes(short replication, IEnumerable
			<StorageType> chosen, EnumSet<StorageType> unavailables, bool isNewBlock)
		{
			IList<StorageType> excess = new List<StorageType>();
			IList<StorageType> storageTypes = ChooseStorageTypes(replication, chosen, excess);
			int expectedSize = storageTypes.Count - excess.Count;
			IList<StorageType> removed = new List<StorageType>();
			for (int i = storageTypes.Count - 1; i >= 0; i--)
			{
				// replace/remove unavailable storage types.
				StorageType t = storageTypes[i];
				if (unavailables.Contains(t))
				{
					StorageType fallback = isNewBlock ? GetCreationFallback(unavailables) : GetReplicationFallback
						(unavailables);
					if (fallback == null)
					{
						removed.AddItem(storageTypes.Remove(i));
					}
					else
					{
						storageTypes.Set(i, fallback);
					}
				}
			}
			// remove excess storage types after fallback replacement.
			Diff(storageTypes, excess, null);
			if (storageTypes.Count < expectedSize)
			{
				Log.Warn("Failed to place enough replicas: expected size is " + expectedSize + " but only "
					 + storageTypes.Count + " storage types can be selected " + "(replication=" + replication
					 + ", selected=" + storageTypes + ", unavailable=" + unavailables + ", removed="
					 + removed + ", policy=" + this + ")");
			}
			return storageTypes;
		}

		/// <summary>
		/// Compute the difference between two lists t and c so that after the diff
		/// computation we have: t = t - c;
		/// Further, if e is not null, set e = e + c - t;
		/// </summary>
		private static void Diff(IList<StorageType> t, IEnumerable<StorageType> c, IList<
			StorageType> e)
		{
			foreach (StorageType storagetype in c)
			{
				int i = t.IndexOf(storagetype);
				if (i >= 0)
				{
					t.Remove(i);
				}
				else
				{
					if (e != null)
					{
						e.AddItem(storagetype);
					}
				}
			}
		}

		/// <summary>
		/// Choose excess storage types for deletion, given the
		/// replication number and the storage types of the chosen replicas.
		/// </summary>
		/// <param name="replication">the replication number.</param>
		/// <param name="chosen">the storage types of the chosen replicas.</param>
		/// <returns>
		/// a list of
		/// <see cref="Org.Apache.Hadoop.FS.StorageType"/>
		/// s for deletion.
		/// </returns>
		public virtual IList<StorageType> ChooseExcess(short replication, IEnumerable<StorageType
			> chosen)
		{
			IList<StorageType> types = ChooseStorageTypes(replication);
			IList<StorageType> excess = new List<StorageType>();
			Diff(types, chosen, excess);
			return excess;
		}

		/// <returns>
		/// the fallback
		/// <see cref="Org.Apache.Hadoop.FS.StorageType"/>
		/// for creation.
		/// </returns>
		public virtual StorageType GetCreationFallback(EnumSet<StorageType> unavailables)
		{
			return GetFallback(unavailables, creationFallbacks);
		}

		/// <returns>
		/// the fallback
		/// <see cref="Org.Apache.Hadoop.FS.StorageType"/>
		/// for replication.
		/// </returns>
		public virtual StorageType GetReplicationFallback(EnumSet<StorageType> unavailables
			)
		{
			return GetFallback(unavailables, replicationFallbacks);
		}

		public override int GetHashCode()
		{
			return byte.ValueOf(id).GetHashCode();
		}

		public override bool Equals(object obj)
		{
			if (obj == this)
			{
				return true;
			}
			else
			{
				if (obj == null || !(obj is Org.Apache.Hadoop.Hdfs.Protocol.BlockStoragePolicy))
				{
					return false;
				}
			}
			Org.Apache.Hadoop.Hdfs.Protocol.BlockStoragePolicy that = (Org.Apache.Hadoop.Hdfs.Protocol.BlockStoragePolicy
				)obj;
			return this.id == that.id;
		}

		public override string ToString()
		{
			return GetType().Name + "{" + name + ":" + id + ", storageTypes=" + Arrays.AsList
				(storageTypes) + ", creationFallbacks=" + Arrays.AsList(creationFallbacks) + ", replicationFallbacks="
				 + Arrays.AsList(replicationFallbacks) + "}";
		}

		public virtual byte GetId()
		{
			return id;
		}

		public virtual string GetName()
		{
			return name;
		}

		public virtual StorageType[] GetStorageTypes()
		{
			return this.storageTypes;
		}

		public virtual StorageType[] GetCreationFallbacks()
		{
			return this.creationFallbacks;
		}

		public virtual StorageType[] GetReplicationFallbacks()
		{
			return this.replicationFallbacks;
		}

		private static StorageType GetFallback(EnumSet<StorageType> unavailables, StorageType
			[] fallbacks)
		{
			foreach (StorageType fb in fallbacks)
			{
				if (!unavailables.Contains(fb))
				{
					return fb;
				}
			}
			return null;
		}

		public virtual bool IsCopyOnCreateFile()
		{
			return copyOnCreateFile;
		}
	}
}
