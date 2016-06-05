using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;


namespace Org.Apache.Hadoop.FS.Viewfs
{
	/// <summary>
	/// This class is needed to address the  problem described in
	/// <see cref="ViewFileSystem.GetFileStatus(Org.Apache.Hadoop.FS.Path)"/>
	/// and
	/// <see cref="ViewFs.GetFileStatus(Org.Apache.Hadoop.FS.Path)"/>
	/// </summary>
	internal class ViewFsFileStatus : FileStatus
	{
		internal readonly FileStatus myFs;

		internal Path modifiedPath;

		internal ViewFsFileStatus(FileStatus fs, Path newPath)
		{
			myFs = fs;
			modifiedPath = newPath;
		}

		public override bool Equals(object o)
		{
			return base.Equals(o);
		}

		public override int GetHashCode()
		{
			return base.GetHashCode();
		}

		public override long GetLen()
		{
			return myFs.GetLen();
		}

		public override bool IsFile()
		{
			return myFs.IsFile();
		}

		public override bool IsDirectory()
		{
			return myFs.IsDirectory();
		}

		public override bool IsDir()
		{
			return myFs.IsDirectory();
		}

		public override bool IsSymlink()
		{
			return myFs.IsSymlink();
		}

		public override long GetBlockSize()
		{
			return myFs.GetBlockSize();
		}

		public override short GetReplication()
		{
			return myFs.GetReplication();
		}

		public override long GetModificationTime()
		{
			return myFs.GetModificationTime();
		}

		public override long GetAccessTime()
		{
			return myFs.GetAccessTime();
		}

		public override FsPermission GetPermission()
		{
			return myFs.GetPermission();
		}

		public override string GetOwner()
		{
			return myFs.GetOwner();
		}

		public override string GetGroup()
		{
			return myFs.GetGroup();
		}

		public override Path GetPath()
		{
			return modifiedPath;
		}

		public override void SetPath(Path p)
		{
			modifiedPath = p;
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path GetSymlink()
		{
			return myFs.GetSymlink();
		}
	}
}
