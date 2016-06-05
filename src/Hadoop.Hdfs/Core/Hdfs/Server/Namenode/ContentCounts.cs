using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// The counter to be computed for content types such as file, directory and symlink,
	/// and the storage type usage such as SSD, DISK, ARCHIVE.
	/// </summary>
	public class ContentCounts
	{
		private EnumCounters<Content> contents;

		private EnumCounters<StorageType> types;

		public class Builder
		{
			private EnumCounters<Content> contents;

			private EnumCounters<StorageType> types;

			public Builder()
			{
				// storage spaces used by corresponding storage types
				contents = new EnumCounters<Content>(typeof(Content));
				types = new EnumCounters<StorageType>(typeof(StorageType));
			}

			public virtual ContentCounts.Builder File(long file)
			{
				contents.Set(Content.File, file);
				return this;
			}

			public virtual ContentCounts.Builder Directory(long directory)
			{
				contents.Set(Content.Directory, directory);
				return this;
			}

			public virtual ContentCounts.Builder Symlink(long symlink)
			{
				contents.Set(Content.Symlink, symlink);
				return this;
			}

			public virtual ContentCounts.Builder Length(long length)
			{
				contents.Set(Content.Length, length);
				return this;
			}

			public virtual ContentCounts.Builder Storagespace(long storagespace)
			{
				contents.Set(Content.Diskspace, storagespace);
				return this;
			}

			public virtual ContentCounts.Builder Snapshot(long snapshot)
			{
				contents.Set(Content.Snapshot, snapshot);
				return this;
			}

			public virtual ContentCounts.Builder Snapshotable_directory(long snapshotable_directory
				)
			{
				contents.Set(Content.SnapshottableDirectory, snapshotable_directory);
				return this;
			}

			public virtual ContentCounts Build()
			{
				return new ContentCounts(contents, types);
			}
		}

		private ContentCounts(EnumCounters<Content> contents, EnumCounters<StorageType> types
			)
		{
			this.contents = contents;
			this.types = types;
		}

		// Get the number of files.
		public virtual long GetFileCount()
		{
			return contents.Get(Content.File);
		}

		// Get the number of directories.
		public virtual long GetDirectoryCount()
		{
			return contents.Get(Content.Directory);
		}

		// Get the number of symlinks.
		public virtual long GetSymlinkCount()
		{
			return contents.Get(Content.Symlink);
		}

		// Get the total of file length in bytes.
		public virtual long GetLength()
		{
			return contents.Get(Content.Length);
		}

		// Get the total of storage space usage in bytes including replication.
		public virtual long GetStoragespace()
		{
			return contents.Get(Content.Diskspace);
		}

		// Get the number of snapshots
		public virtual long GetSnapshotCount()
		{
			return contents.Get(Content.Snapshot);
		}

		// Get the number of snapshottable directories.
		public virtual long GetSnapshotableDirectoryCount()
		{
			return contents.Get(Content.SnapshottableDirectory);
		}

		public virtual long[] GetTypeSpaces()
		{
			return types.AsArray();
		}

		public virtual long GetTypeSpace(StorageType t)
		{
			return types.Get(t);
		}

		public virtual void AddContent(Content c, long val)
		{
			contents.Add(c, val);
		}

		public virtual void AddContents(ContentCounts that)
		{
			contents.Add(that.contents);
			types.Add(that.types);
		}

		public virtual void AddTypeSpace(StorageType t, long val)
		{
			types.Add(t, val);
		}

		public virtual void AddTypeSpaces(EnumCounters<StorageType> that)
		{
			this.types.Add(that);
		}
	}
}
