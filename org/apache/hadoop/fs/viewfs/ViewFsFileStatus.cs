using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	/// <summary>
	/// This class is needed to address the  problem described in
	/// <see cref="ViewFileSystem.getFileStatus(org.apache.hadoop.fs.Path)"/>
	/// and
	/// <see cref="ViewFs.getFileStatus(org.apache.hadoop.fs.Path)"/>
	/// </summary>
	internal class ViewFsFileStatus : org.apache.hadoop.fs.FileStatus
	{
		internal readonly org.apache.hadoop.fs.FileStatus myFs;

		internal org.apache.hadoop.fs.Path modifiedPath;

		internal ViewFsFileStatus(org.apache.hadoop.fs.FileStatus fs, org.apache.hadoop.fs.Path
			 newPath)
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

		public override long getLen()
		{
			return myFs.getLen();
		}

		public override bool isFile()
		{
			return myFs.isFile();
		}

		public override bool isDirectory()
		{
			return myFs.isDirectory();
		}

		public override bool isDir()
		{
			return myFs.isDirectory();
		}

		public override bool isSymlink()
		{
			return myFs.isSymlink();
		}

		public override long getBlockSize()
		{
			return myFs.getBlockSize();
		}

		public override short getReplication()
		{
			return myFs.getReplication();
		}

		public override long getModificationTime()
		{
			return myFs.getModificationTime();
		}

		public override long getAccessTime()
		{
			return myFs.getAccessTime();
		}

		public override org.apache.hadoop.fs.permission.FsPermission getPermission()
		{
			return myFs.getPermission();
		}

		public override string getOwner()
		{
			return myFs.getOwner();
		}

		public override string getGroup()
		{
			return myFs.getGroup();
		}

		public override org.apache.hadoop.fs.Path getPath()
		{
			return modifiedPath;
		}

		public override void setPath(org.apache.hadoop.fs.Path p)
		{
			modifiedPath = p;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path getSymlink()
		{
			return myFs.getSymlink();
		}
	}
}
