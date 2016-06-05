using System;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Encapsulates the URI and storage medium that together describe a
	/// storage directory.
	/// </summary>
	/// <remarks>
	/// Encapsulates the URI and storage medium that together describe a
	/// storage directory.
	/// The default storage medium is assumed to be DISK, if none is specified.
	/// </remarks>
	public class StorageLocation
	{
		internal readonly StorageType storageType;

		internal readonly FilePath file;

		/// <summary>Regular expression that describes a storage uri with a storage type.</summary>
		/// <remarks>
		/// Regular expression that describes a storage uri with a storage type.
		/// e.g. [Disk]/storages/storage1/
		/// </remarks>
		private static readonly Sharpen.Pattern regex = Sharpen.Pattern.Compile("^\\[(\\w*)\\](.+)$"
			);

		private StorageLocation(StorageType storageType, URI uri)
		{
			this.storageType = storageType;
			if (uri.GetScheme() == null || Sharpen.Runtime.EqualsIgnoreCase("file", uri.GetScheme
				()))
			{
				// drop any (illegal) authority in the URI for backwards compatibility
				this.file = new FilePath(uri.GetPath());
			}
			else
			{
				throw new ArgumentException("Unsupported URI schema in " + uri);
			}
		}

		public virtual StorageType GetStorageType()
		{
			return this.storageType;
		}

		internal virtual URI GetUri()
		{
			return file.ToURI();
		}

		public virtual FilePath GetFile()
		{
			return this.file;
		}

		/// <summary>Attempt to parse a storage uri with storage class and URI.</summary>
		/// <remarks>
		/// Attempt to parse a storage uri with storage class and URI. The storage
		/// class component of the uri is case-insensitive.
		/// </remarks>
		/// <param name="rawLocation">
		/// Location string of the format [type]uri, where [type] is
		/// optional.
		/// </param>
		/// <returns>
		/// A StorageLocation object if successfully parsed, null otherwise.
		/// Does not throw any exceptions.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Security.SecurityException"/>
		public static Org.Apache.Hadoop.Hdfs.Server.Datanode.StorageLocation Parse(string
			 rawLocation)
		{
			Matcher matcher = regex.Matcher(rawLocation);
			StorageType storageType = StorageType.Default;
			string location = rawLocation;
			if (matcher.Matches())
			{
				string classString = matcher.Group(1);
				location = matcher.Group(2);
				if (!classString.IsEmpty())
				{
					storageType = StorageType.ValueOf(StringUtils.ToUpperCase(classString));
				}
			}
			return new Org.Apache.Hadoop.Hdfs.Server.Datanode.StorageLocation(storageType, new 
				Path(location).ToUri());
		}

		public override string ToString()
		{
			return "[" + storageType + "]" + file.ToURI();
		}
	}
}
