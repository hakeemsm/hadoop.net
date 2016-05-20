using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	internal class ViewFsLocatedFileStatus : org.apache.hadoop.fs.LocatedFileStatus
	{
		internal readonly org.apache.hadoop.fs.LocatedFileStatus myFs;

		internal org.apache.hadoop.fs.Path modifiedPath;

		internal ViewFsLocatedFileStatus(org.apache.hadoop.fs.LocatedFileStatus locatedFileStatus
			, org.apache.hadoop.fs.Path path)
		{
			myFs = locatedFileStatus;
			modifiedPath = path;
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

		public override void setSymlink(org.apache.hadoop.fs.Path p)
		{
			myFs.setSymlink(p);
		}

		public override org.apache.hadoop.fs.BlockLocation[] getBlockLocations()
		{
			return myFs.getBlockLocations();
		}

		public override int compareTo(object o)
		{
			return base.compareTo(o);
		}

		public override bool Equals(object o)
		{
			return base.Equals(o);
		}

		public override int GetHashCode()
		{
			return base.GetHashCode();
		}
	}
}
