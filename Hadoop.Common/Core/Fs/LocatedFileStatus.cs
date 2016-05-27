using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>This class defines a FileStatus that includes a file's block locations.</summary>
	public class LocatedFileStatus : FileStatus
	{
		private BlockLocation[] locations;

		public LocatedFileStatus()
			: base()
		{
		}

		/// <summary>Constructor</summary>
		/// <param name="stat">a file status</param>
		/// <param name="locations">a file's block locations</param>
		/// <exception cref="System.IO.IOException"/>
		public LocatedFileStatus(FileStatus stat, BlockLocation[] locations)
			: this(stat.GetLen(), stat.IsDirectory(), stat.GetReplication(), stat.GetBlockSize
				(), stat.GetModificationTime(), stat.GetAccessTime(), stat.GetPermission(), stat
				.GetOwner(), stat.GetGroup(), null, stat.GetPath(), locations)
		{
			if (stat.IsSymlink())
			{
				SetSymlink(stat.GetSymlink());
			}
		}

		/// <summary>Constructor</summary>
		/// <param name="length">a file's length</param>
		/// <param name="isdir">if the path is a directory</param>
		/// <param name="block_replication">the file's replication factor</param>
		/// <param name="blocksize">a file's block size</param>
		/// <param name="modification_time">a file's modification time</param>
		/// <param name="access_time">a file's access time</param>
		/// <param name="permission">a file's permission</param>
		/// <param name="owner">a file's owner</param>
		/// <param name="group">a file's group</param>
		/// <param name="symlink">symlink if the path is a symbolic link</param>
		/// <param name="path">the path's qualified name</param>
		/// <param name="locations">a file's block locations</param>
		public LocatedFileStatus(long length, bool isdir, int block_replication, long blocksize
			, long modification_time, long access_time, FsPermission permission, string owner
			, string group, Path symlink, Path path, BlockLocation[] locations)
			: base(length, isdir, block_replication, blocksize, modification_time, access_time
				, permission, owner, group, symlink, path)
		{
			this.locations = locations;
		}

		/// <summary>Get the file's block locations</summary>
		/// <returns>the file's block locations</returns>
		public virtual BlockLocation[] GetBlockLocations()
		{
			return locations;
		}

		/// <summary>Compare this object to another object</summary>
		/// <param name="o">the object to be compared.</param>
		/// <returns>
		/// a negative integer, zero, or a positive integer as this object
		/// is less than, equal to, or greater than the specified object.
		/// </returns>
		/// <exception cref="System.InvalidCastException">
		/// if the specified object's is not of
		/// type FileStatus
		/// </exception>
		public override int CompareTo(object o)
		{
			return base.CompareTo(o);
		}

		/// <summary>Compare if this object is equal to another object</summary>
		/// <param name="o">the object to be compared.</param>
		/// <returns>true if two file status has the same path name; false if not.</returns>
		public override bool Equals(object o)
		{
			return base.Equals(o);
		}

		/// <summary>
		/// Returns a hash code value for the object, which is defined as
		/// the hash code of the path name.
		/// </summary>
		/// <returns>a hash code value for the path name.</returns>
		public override int GetHashCode()
		{
			return base.GetHashCode();
		}
	}
}
