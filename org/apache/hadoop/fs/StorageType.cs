using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Defines the types of supported storage media.</summary>
	/// <remarks>
	/// Defines the types of supported storage media. The default storage
	/// medium is assumed to be DISK.
	/// </remarks>
	[System.Serializable]
	public sealed class StorageType
	{
		public static readonly org.apache.hadoop.fs.StorageType RAM_DISK = new org.apache.hadoop.fs.StorageType
			(true);

		public static readonly org.apache.hadoop.fs.StorageType SSD = new org.apache.hadoop.fs.StorageType
			(false);

		public static readonly org.apache.hadoop.fs.StorageType DISK = new org.apache.hadoop.fs.StorageType
			(false);

		public static readonly org.apache.hadoop.fs.StorageType ARCHIVE = new org.apache.hadoop.fs.StorageType
			(false);

		private readonly bool isTransient;

		public static readonly org.apache.hadoop.fs.StorageType DEFAULT = org.apache.hadoop.fs.StorageType
			.DISK;

		public static readonly org.apache.hadoop.fs.StorageType[] EMPTY_ARRAY = new org.apache.hadoop.fs.StorageType
			[] {  };

		private static readonly org.apache.hadoop.fs.StorageType[] VALUES = values();

		internal StorageType(bool isTransient)
		{
			// sorted by the speed of the storage types, from fast to slow
			this.isTransient = isTransient;
		}

		public bool isTransient()
		{
			return org.apache.hadoop.fs.StorageType.isTransient;
		}

		public bool supportTypeQuota()
		{
			return !org.apache.hadoop.fs.StorageType.isTransient;
		}

		public bool isMovable()
		{
			return !org.apache.hadoop.fs.StorageType.isTransient;
		}

		public static System.Collections.Generic.IList<org.apache.hadoop.fs.StorageType> 
			asList()
		{
			return java.util.Arrays.asList(org.apache.hadoop.fs.StorageType.VALUES);
		}

		public static System.Collections.Generic.IList<org.apache.hadoop.fs.StorageType> 
			getMovableTypes()
		{
			return getNonTransientTypes();
		}

		public static System.Collections.Generic.IList<org.apache.hadoop.fs.StorageType> 
			getTypesSupportingQuota()
		{
			return getNonTransientTypes();
		}

		public static org.apache.hadoop.fs.StorageType parseStorageType(int i)
		{
			return org.apache.hadoop.fs.StorageType.VALUES[i];
		}

		public static org.apache.hadoop.fs.StorageType parseStorageType(string s)
		{
			return org.apache.hadoop.fs.StorageType.valueOf(org.apache.hadoop.util.StringUtils
				.toUpperCase(s));
		}

		private static System.Collections.Generic.IList<org.apache.hadoop.fs.StorageType>
			 getNonTransientTypes()
		{
			System.Collections.Generic.IList<org.apache.hadoop.fs.StorageType> nonTransientTypes
				 = new System.Collections.Generic.List<org.apache.hadoop.fs.StorageType>();
			foreach (org.apache.hadoop.fs.StorageType t in org.apache.hadoop.fs.StorageType.VALUES)
			{
				if (t.isTransient == false)
				{
					nonTransientTypes.add(t);
				}
			}
			return nonTransientTypes;
		}
	}
}
