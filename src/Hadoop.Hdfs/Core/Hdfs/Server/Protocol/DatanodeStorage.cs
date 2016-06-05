using System;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>Class captures information of a storage in Datanode.</summary>
	public class DatanodeStorage
	{
		/// <summary>The state of the storage.</summary>
		public enum State
		{
			Normal,
			ReadOnlyShared,
			Failed
		}

		private readonly string storageID;

		private readonly DatanodeStorage.State state;

		private readonly StorageType storageType;

		private const string StorageIdPrefix = "DS-";

		/// <summary>
		/// Create a storage with
		/// <see cref="State.Normal"/>
		/// and
		/// <see cref="Org.Apache.Hadoop.FS.StorageType.Default"/>
		/// .
		/// </summary>
		public DatanodeStorage(string storageID)
			: this(storageID, DatanodeStorage.State.Normal, StorageType.Default)
		{
		}

		public DatanodeStorage(string sid, DatanodeStorage.State s, StorageType sm)
		{
			this.storageID = sid;
			this.state = s;
			this.storageType = sm;
		}

		public virtual string GetStorageID()
		{
			return storageID;
		}

		public virtual DatanodeStorage.State GetState()
		{
			return state;
		}

		public virtual StorageType GetStorageType()
		{
			return storageType;
		}

		/// <summary>Generate new storage ID.</summary>
		/// <remarks>
		/// Generate new storage ID. The format of this string can be changed
		/// in the future without requiring that old storage IDs be updated.
		/// </remarks>
		/// <returns>unique storage ID</returns>
		public static string GenerateUuid()
		{
			return StorageIdPrefix + UUID.RandomUUID();
		}

		/// <summary>Verify that a given string is a storage ID in the "DS-..uuid.." format.</summary>
		public static bool IsValidStorageId(string storageID)
		{
			try
			{
				// Attempt to parse the UUID.
				if (storageID != null && storageID.IndexOf(StorageIdPrefix) == 0)
				{
					UUID.FromString(Sharpen.Runtime.Substring(storageID, StorageIdPrefix.Length));
					return true;
				}
			}
			catch (ArgumentException)
			{
			}
			return false;
		}

		public override string ToString()
		{
			return "DatanodeStorage[" + storageID + "," + storageType + "," + state + "]";
		}

		public override bool Equals(object other)
		{
			if (other == this)
			{
				return true;
			}
			if ((other == null) || !(other is Org.Apache.Hadoop.Hdfs.Server.Protocol.DatanodeStorage
				))
			{
				return false;
			}
			Org.Apache.Hadoop.Hdfs.Server.Protocol.DatanodeStorage otherStorage = (Org.Apache.Hadoop.Hdfs.Server.Protocol.DatanodeStorage
				)other;
			return string.CompareOrdinal(otherStorage.GetStorageID(), GetStorageID()) == 0;
		}

		public override int GetHashCode()
		{
			return GetStorageID().GetHashCode();
		}
	}
}
