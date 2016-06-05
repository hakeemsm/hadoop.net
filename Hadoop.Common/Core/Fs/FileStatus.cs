using System;
using System.IO;
using System.Text;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>Interface that represents the client side information for a file.</summary>
	public class FileStatus : IWritable, IComparable
	{
		private Path path;

		private long length;

		private bool isdir;

		private short block_replication;

		private long blocksize;

		private long modification_time;

		private long access_time;

		private FsPermission permission;

		private string owner;

		private string group;

		private Path symlink;

		public FileStatus()
			: this(0, false, 0, 0, 0, 0, null, null, null, null)
		{
		}

		public FileStatus(long length, bool isdir, int block_replication, long blocksize, 
			long modification_time, Path path)
			: this(length, isdir, block_replication, blocksize, modification_time, 0, null, null
				, null, path)
		{
		}

		/// <summary>Constructor for file systems on which symbolic links are not supported</summary>
		public FileStatus(long length, bool isdir, int block_replication, long blocksize, 
			long modification_time, long access_time, FsPermission permission, string owner, 
			string group, Path path)
			: this(length, isdir, block_replication, blocksize, modification_time, access_time
				, permission, owner, group, null, path)
		{
		}

		public FileStatus(long length, bool isdir, int block_replication, long blocksize, 
			long modification_time, long access_time, FsPermission permission, string owner, 
			string group, Path symlink, Path path)
		{
			//We should deprecate this soon?
			this.length = length;
			this.isdir = isdir;
			this.block_replication = (short)block_replication;
			this.blocksize = blocksize;
			this.modification_time = modification_time;
			this.access_time = access_time;
			if (permission != null)
			{
				this.permission = permission;
			}
			else
			{
				if (isdir)
				{
					this.permission = FsPermission.GetDirDefault();
				}
				else
				{
					if (symlink != null)
					{
						this.permission = FsPermission.GetDefault();
					}
					else
					{
						this.permission = FsPermission.GetFileDefault();
					}
				}
			}
			this.owner = (owner == null) ? string.Empty : owner;
			this.group = (group == null) ? string.Empty : group;
			this.symlink = symlink;
			this.path = path;
			// The variables isdir and symlink indicate the type:
			// 1. isdir implies directory, in which case symlink must be null.
			// 2. !isdir implies a file or symlink, symlink != null implies a
			//    symlink, otherwise it's a file.
			System.Diagnostics.Debug.Assert((isdir && symlink == null) || !isdir);
		}

		/// <summary>Copy constructor.</summary>
		/// <param name="other">FileStatus to copy</param>
		/// <exception cref="System.IO.IOException"/>
		public FileStatus(Org.Apache.Hadoop.FS.FileStatus other)
			: this(other.GetLen(), other.IsDirectory(), other.GetReplication(), other.GetBlockSize
				(), other.GetModificationTime(), other.GetAccessTime(), other.GetPermission(), other
				.GetOwner(), other.GetGroup(), (other.IsSymlink() ? other.GetSymlink() : null), 
				other.GetPath())
		{
		}

		// It's important to call the getters here instead of directly accessing the
		// members.  Subclasses like ViewFsFileStatus can override the getters.
		/// <summary>Get the length of this file, in bytes.</summary>
		/// <returns>the length of this file, in bytes.</returns>
		public virtual long GetLen()
		{
			return length;
		}

		/// <summary>Is this a file?</summary>
		/// <returns>true if this is a file</returns>
		public virtual bool IsFile()
		{
			return !isdir && !IsSymlink();
		}

		/// <summary>Is this a directory?</summary>
		/// <returns>true if this is a directory</returns>
		public virtual bool IsDirectory()
		{
			return isdir;
		}

		/// <summary>
		/// Old interface, instead use the explicit
		/// <see cref="IsFile()"/>
		/// ,
		/// <see cref="IsDirectory()"/>
		/// , and
		/// <see cref="IsSymlink()"/>
		/// 
		/// </summary>
		/// <returns>true if this is a directory.</returns>
		[System.ObsoleteAttribute(@"Use IsFile() ,  IsDirectory() , and IsSymlink() instead."
			)]
		public virtual bool IsDir()
		{
			return isdir;
		}

		/// <summary>Is this a symbolic link?</summary>
		/// <returns>true if this is a symbolic link</returns>
		public virtual bool IsSymlink()
		{
			return symlink != null;
		}

		/// <summary>Get the block size of the file.</summary>
		/// <returns>the number of bytes</returns>
		public virtual long GetBlockSize()
		{
			return blocksize;
		}

		/// <summary>Get the replication factor of a file.</summary>
		/// <returns>the replication factor of a file.</returns>
		public virtual short GetReplication()
		{
			return block_replication;
		}

		/// <summary>Get the modification time of the file.</summary>
		/// <returns>the modification time of file in milliseconds since January 1, 1970 UTC.
		/// 	</returns>
		public virtual long GetModificationTime()
		{
			return modification_time;
		}

		/// <summary>Get the access time of the file.</summary>
		/// <returns>the access time of file in milliseconds since January 1, 1970 UTC.</returns>
		public virtual long GetAccessTime()
		{
			return access_time;
		}

		/// <summary>Get FsPermission associated with the file.</summary>
		/// <returns>
		/// permssion. If a filesystem does not have a notion of permissions
		/// or if permissions could not be determined, then default
		/// permissions equivalent of "rwxrwxrwx" is returned.
		/// </returns>
		public virtual FsPermission GetPermission()
		{
			return permission;
		}

		/// <summary>Tell whether the underlying file or directory is encrypted or not.</summary>
		/// <returns>true if the underlying file is encrypted.</returns>
		public virtual bool IsEncrypted()
		{
			return permission.GetEncryptedBit();
		}

		/// <summary>Get the owner of the file.</summary>
		/// <returns>
		/// owner of the file. The string could be empty if there is no
		/// notion of owner of a file in a filesystem or if it could not
		/// be determined (rare).
		/// </returns>
		public virtual string GetOwner()
		{
			return owner;
		}

		/// <summary>Get the group associated with the file.</summary>
		/// <returns>
		/// group for the file. The string could be empty if there is no
		/// notion of group of a file in a filesystem or if it could not
		/// be determined (rare).
		/// </returns>
		public virtual string GetGroup()
		{
			return group;
		}

		public virtual Path GetPath()
		{
			return path;
		}

		public virtual void SetPath(Path p)
		{
			path = p;
		}

		/* These are provided so that these values could be loaded lazily
		* by a filesystem (e.g. local file system).
		*/
		/// <summary>Sets permission.</summary>
		/// <param name="permission">if permission is null, default value is set</param>
		protected internal virtual void SetPermission(FsPermission permission)
		{
			this.permission = (permission == null) ? FsPermission.GetFileDefault() : permission;
		}

		/// <summary>Sets owner.</summary>
		/// <param name="owner">if it is null, default value is set</param>
		protected internal virtual void SetOwner(string owner)
		{
			this.owner = (owner == null) ? string.Empty : owner;
		}

		/// <summary>Sets group.</summary>
		/// <param name="group">if it is null, default value is set</param>
		protected internal virtual void SetGroup(string group)
		{
			this.group = (group == null) ? string.Empty : group;
		}

		/// <returns>The contents of the symbolic link.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetSymlink()
		{
			if (!IsSymlink())
			{
				throw new IOException("Path " + path + " is not a symbolic link");
			}
			return symlink;
		}

		public virtual void SetSymlink(Path p)
		{
			symlink = p;
		}

		//////////////////////////////////////////////////
		// Writable
		//////////////////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter @out)
		{
			Text.WriteString(@out, GetPath().ToString(), Text.DefaultMaxLen);
			@out.WriteLong(GetLen());
			@out.WriteBoolean(IsDirectory());
			@out.WriteShort(GetReplication());
			@out.WriteLong(GetBlockSize());
			@out.WriteLong(GetModificationTime());
			@out.WriteLong(GetAccessTime());
			GetPermission().Write(@out);
			Text.WriteString(@out, GetOwner(), Text.DefaultMaxLen);
			Text.WriteString(@out, GetGroup(), Text.DefaultMaxLen);
			@out.WriteBoolean(IsSymlink());
			if (IsSymlink())
			{
				Text.WriteString(@out, GetSymlink().ToString(), Text.DefaultMaxLen);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			string strPath = Text.ReadString(@in, Text.DefaultMaxLen);
			this.path = new Path(strPath);
			this.length = @in.ReadLong();
			this.isdir = @in.ReadBoolean();
			this.block_replication = @in.ReadShort();
			blocksize = @in.ReadLong();
			modification_time = @in.ReadLong();
			access_time = @in.ReadLong();
			permission.ReadFields(@in);
			owner = Text.ReadString(@in, Text.DefaultMaxLen);
			group = Text.ReadString(@in, Text.DefaultMaxLen);
			if (@in.ReadBoolean())
			{
				this.symlink = new Path(Text.ReadString(@in, Text.DefaultMaxLen));
			}
			else
			{
				this.symlink = null;
			}
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
		public virtual int CompareTo(object o)
		{
			Org.Apache.Hadoop.FS.FileStatus other = (Org.Apache.Hadoop.FS.FileStatus)o;
			return this.GetPath().CompareTo(other.GetPath());
		}

		/// <summary>Compare if this object is equal to another object</summary>
		/// <param name="o">the object to be compared.</param>
		/// <returns>true if two file status has the same path name; false if not.</returns>
		public override bool Equals(object o)
		{
			if (o == null)
			{
				return false;
			}
			if (this == o)
			{
				return true;
			}
			if (!(o is Org.Apache.Hadoop.FS.FileStatus))
			{
				return false;
			}
			Org.Apache.Hadoop.FS.FileStatus other = (Org.Apache.Hadoop.FS.FileStatus)o;
			return this.GetPath().Equals(other.GetPath());
		}

		/// <summary>
		/// Returns a hash code value for the object, which is defined as
		/// the hash code of the path name.
		/// </summary>
		/// <returns>a hash code value for the path name.</returns>
		public override int GetHashCode()
		{
			return GetPath().GetHashCode();
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(GetType().Name);
			sb.Append("{");
			sb.Append("path=" + path);
			sb.Append("; isDirectory=" + isdir);
			if (!IsDirectory())
			{
				sb.Append("; length=" + length);
				sb.Append("; replication=" + block_replication);
				sb.Append("; blocksize=" + blocksize);
			}
			sb.Append("; modification_time=" + modification_time);
			sb.Append("; access_time=" + access_time);
			sb.Append("; owner=" + owner);
			sb.Append("; group=" + group);
			sb.Append("; permission=" + permission);
			sb.Append("; isSymlink=" + IsSymlink());
			if (IsSymlink())
			{
				sb.Append("; symlink=" + symlink);
			}
			sb.Append("}");
			return sb.ToString();
		}
	}
}
