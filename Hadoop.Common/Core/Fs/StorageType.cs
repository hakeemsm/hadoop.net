using System.Collections.Generic;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Defines the types of supported storage media.</summary>
	/// <remarks>
	/// Defines the types of supported storage media. The default storage
	/// medium is assumed to be DISK.
	/// </remarks>
	[System.Serializable]
	public sealed class StorageType
	{
		public static readonly Org.Apache.Hadoop.FS.StorageType RamDisk = new Org.Apache.Hadoop.FS.StorageType
			(true);

		public static readonly Org.Apache.Hadoop.FS.StorageType Ssd = new Org.Apache.Hadoop.FS.StorageType
			(false);

		public static readonly Org.Apache.Hadoop.FS.StorageType Disk = new Org.Apache.Hadoop.FS.StorageType
			(false);

		public static readonly Org.Apache.Hadoop.FS.StorageType Archive = new Org.Apache.Hadoop.FS.StorageType
			(false);

		private readonly bool isTransient;

		public static readonly Org.Apache.Hadoop.FS.StorageType Default = Org.Apache.Hadoop.FS.StorageType
			.Disk;

		public static readonly Org.Apache.Hadoop.FS.StorageType[] EmptyArray = new Org.Apache.Hadoop.FS.StorageType
			[] {  };

		private static readonly Org.Apache.Hadoop.FS.StorageType[] Values = Values();

		internal StorageType(bool isTransient)
		{
			// sorted by the speed of the storage types, from fast to slow
			this.isTransient = isTransient;
		}

		public bool IsTransient()
		{
			return Org.Apache.Hadoop.FS.StorageType.isTransient;
		}

		public bool SupportTypeQuota()
		{
			return !Org.Apache.Hadoop.FS.StorageType.isTransient;
		}

		public bool IsMovable()
		{
			return !Org.Apache.Hadoop.FS.StorageType.isTransient;
		}

		public static IList<Org.Apache.Hadoop.FS.StorageType> AsList()
		{
			return Arrays.AsList(Org.Apache.Hadoop.FS.StorageType.Values);
		}

		public static IList<Org.Apache.Hadoop.FS.StorageType> GetMovableTypes()
		{
			return GetNonTransientTypes();
		}

		public static IList<Org.Apache.Hadoop.FS.StorageType> GetTypesSupportingQuota()
		{
			return GetNonTransientTypes();
		}

		public static Org.Apache.Hadoop.FS.StorageType ParseStorageType(int i)
		{
			return Org.Apache.Hadoop.FS.StorageType.Values[i];
		}

		public static Org.Apache.Hadoop.FS.StorageType ParseStorageType(string s)
		{
			return Org.Apache.Hadoop.FS.StorageType.ValueOf(StringUtils.ToUpperCase(s));
		}

		private static IList<Org.Apache.Hadoop.FS.StorageType> GetNonTransientTypes()
		{
			IList<Org.Apache.Hadoop.FS.StorageType> nonTransientTypes = new AList<Org.Apache.Hadoop.FS.StorageType
				>();
			foreach (Org.Apache.Hadoop.FS.StorageType t in Org.Apache.Hadoop.FS.StorageType.Values)
			{
				if (t.isTransient == false)
				{
					nonTransientTypes.AddItem(t);
				}
			}
			return nonTransientTypes;
		}
	}
}
