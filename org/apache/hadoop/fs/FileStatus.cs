using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Interface that represents the client side information for a file.</summary>
	public class FileStatus : org.apache.hadoop.io.Writable, java.lang.Comparable
	{
		private org.apache.hadoop.fs.Path path;

		private long length;

		private bool isdir;

		private short block_replication;

		private long blocksize;

		private long modification_time;

		private long access_time;

		private org.apache.hadoop.fs.permission.FsPermission permission;

		private string owner;

		private string group;

		private org.apache.hadoop.fs.Path symlink;

		public FileStatus()
			: this(0, false, 0, 0, 0, 0, null, null, null, null)
		{
		}

		public FileStatus(long length, bool isdir, int block_replication, long blocksize, 
			long modification_time, org.apache.hadoop.fs.Path path)
			: this(length, isdir, block_replication, blocksize, modification_time, 0, null, null
				, null, path)
		{
		}

		/// <summary>Constructor for file systems on which symbolic links are not supported</summary>
		public FileStatus(long length, bool isdir, int block_replication, long blocksize, 
			long modification_time, long access_time, org.apache.hadoop.fs.permission.FsPermission
			 permission, string owner, string group, org.apache.hadoop.fs.Path path)
			: this(length, isdir, block_replication, blocksize, modification_time, access_time
				, permission, owner, group, null, path)
		{
		}

		public FileStatus(long length, bool isdir, int block_replication, long blocksize, 
			long modification_time, long access_time, org.apache.hadoop.fs.permission.FsPermission
			 permission, string owner, string group, org.apache.hadoop.fs.Path symlink, org.apache.hadoop.fs.Path
			 path)
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
					this.permission = org.apache.hadoop.fs.permission.FsPermission.getDirDefault();
				}
				else
				{
					if (symlink != null)
					{
						this.permission = org.apache.hadoop.fs.permission.FsPermission.getDefault();
					}
					else
					{
						this.permission = org.apache.hadoop.fs.permission.FsPermission.getFileDefault();
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
		public FileStatus(org.apache.hadoop.fs.FileStatus other)
			: this(other.getLen(), other.isDirectory(), other.getReplication(), other.getBlockSize
				(), other.getModificationTime(), other.getAccessTime(), other.getPermission(), other
				.getOwner(), other.getGroup(), (other.isSymlink() ? other.getSymlink() : null), 
				other.getPath())
		{
		}

		// It's important to call the getters here instead of directly accessing the
		// members.  Subclasses like ViewFsFileStatus can override the getters.
		/// <summary>Get the length of this file, in bytes.</summary>
		/// <returns>the length of this file, in bytes.</returns>
		public virtual long getLen()
		{
			return length;
		}

		/// <summary>Is this a file?</summary>
		/// <returns>true if this is a file</returns>
		public virtual bool isFile()
		{
			return !isdir && !isSymlink();
		}

		/// <summary>Is this a directory?</summary>
		/// <returns>true if this is a directory</returns>
		public virtual bool isDirectory()
		{
			return isdir;
		}

		/// <summary>
		/// Old interface, instead use the explicit
		/// <see cref="isFile()"/>
		/// ,
		/// <see cref="isDirectory()"/>
		/// , and
		/// <see cref="isSymlink()"/>
		/// 
		/// </summary>
		/// <returns>true if this is a directory.</returns>
		[System.ObsoleteAttribute(@"Use isFile() ,  isDirectory() , and isSymlink() instead."
			)]
		public virtual bool isDir()
		{
			return isdir;
		}

		/// <summary>Is this a symbolic link?</summary>
		/// <returns>true if this is a symbolic link</returns>
		public virtual bool isSymlink()
		{
			return symlink != null;
		}

		/// <summary>Get the block size of the file.</summary>
		/// <returns>the number of bytes</returns>
		public virtual long getBlockSize()
		{
			return blocksize;
		}

		/// <summary>Get the replication factor of a file.</summary>
		/// <returns>the replication factor of a file.</returns>
		public virtual short getReplication()
		{
			return block_replication;
		}

		/// <summary>Get the modification time of the file.</summary>
		/// <returns>the modification time of file in milliseconds since January 1, 1970 UTC.
		/// 	</returns>
		public virtual long getModificationTime()
		{
			return modification_time;
		}

		/// <summary>Get the access time of the file.</summary>
		/// <returns>the access time of file in milliseconds since January 1, 1970 UTC.</returns>
		public virtual long getAccessTime()
		{
			return access_time;
		}

		/// <summary>Get FsPermission associated with the file.</summary>
		/// <returns>
		/// permssion. If a filesystem does not have a notion of permissions
		/// or if permissions could not be determined, then default
		/// permissions equivalent of "rwxrwxrwx" is returned.
		/// </returns>
		public virtual org.apache.hadoop.fs.permission.FsPermission getPermission()
		{
			return permission;
		}

		/// <summary>Tell whether the underlying file or directory is encrypted or not.</summary>
		/// <returns>true if the underlying file is encrypted.</returns>
		public virtual bool isEncrypted()
		{
			return permission.getEncryptedBit();
		}

		/// <summary>Get the owner of the file.</summary>
		/// <returns>
		/// owner of the file. The string could be empty if there is no
		/// notion of owner of a file in a filesystem or if it could not
		/// be determined (rare).
		/// </returns>
		public virtual string getOwner()
		{
			return owner;
		}

		/// <summary>Get the group associated with the file.</summary>
		/// <returns>
		/// group for the file. The string could be empty if there is no
		/// notion of group of a file in a filesystem or if it could not
		/// be determined (rare).
		/// </returns>
		public virtual string getGroup()
		{
			return group;
		}

		public virtual org.apache.hadoop.fs.Path getPath()
		{
			return path;
		}

		public virtual void setPath(org.apache.hadoop.fs.Path p)
		{
			path = p;
		}

		/* These are provided so that these values could be loaded lazily
		* by a filesystem (e.g. local file system).
		*/
		/// <summary>Sets permission.</summary>
		/// <param name="permission">if permission is null, default value is set</param>
		protected internal virtual void setPermission(org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			this.permission = (permission == null) ? org.apache.hadoop.fs.permission.FsPermission
				.getFileDefault() : permission;
		}

		/// <summary>Sets owner.</summary>
		/// <param name="owner">if it is null, default value is set</param>
		protected internal virtual void setOwner(string owner)
		{
			this.owner = (owner == null) ? string.Empty : owner;
		}

		/// <summary>Sets group.</summary>
		/// <param name="group">if it is null, default value is set</param>
		protected internal virtual void setGroup(string group)
		{
			this.group = (group == null) ? string.Empty : group;
		}

		/// <returns>The contents of the symbolic link.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.Path getSymlink()
		{
			if (!isSymlink())
			{
				throw new System.IO.IOException("Path " + path + " is not a symbolic link");
			}
			return symlink;
		}

		public virtual void setSymlink(org.apache.hadoop.fs.Path p)
		{
			symlink = p;
		}

		//////////////////////////////////////////////////
		// Writable
		//////////////////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			org.apache.hadoop.io.Text.writeString(@out, getPath().ToString(), org.apache.hadoop.io.Text
				.DEFAULT_MAX_LEN);
			@out.writeLong(getLen());
			@out.writeBoolean(isDirectory());
			@out.writeShort(getReplication());
			@out.writeLong(getBlockSize());
			@out.writeLong(getModificationTime());
			@out.writeLong(getAccessTime());
			getPermission().write(@out);
			org.apache.hadoop.io.Text.writeString(@out, getOwner(), org.apache.hadoop.io.Text
				.DEFAULT_MAX_LEN);
			org.apache.hadoop.io.Text.writeString(@out, getGroup(), org.apache.hadoop.io.Text
				.DEFAULT_MAX_LEN);
			@out.writeBoolean(isSymlink());
			if (isSymlink())
			{
				org.apache.hadoop.io.Text.writeString(@out, getSymlink().ToString(), org.apache.hadoop.io.Text
					.DEFAULT_MAX_LEN);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			string strPath = org.apache.hadoop.io.Text.readString(@in, org.apache.hadoop.io.Text
				.DEFAULT_MAX_LEN);
			this.path = new org.apache.hadoop.fs.Path(strPath);
			this.length = @in.readLong();
			this.isdir = @in.readBoolean();
			this.block_replication = @in.readShort();
			blocksize = @in.readLong();
			modification_time = @in.readLong();
			access_time = @in.readLong();
			permission.readFields(@in);
			owner = org.apache.hadoop.io.Text.readString(@in, org.apache.hadoop.io.Text.DEFAULT_MAX_LEN
				);
			group = org.apache.hadoop.io.Text.readString(@in, org.apache.hadoop.io.Text.DEFAULT_MAX_LEN
				);
			if (@in.readBoolean())
			{
				this.symlink = new org.apache.hadoop.fs.Path(org.apache.hadoop.io.Text.readString
					(@in, org.apache.hadoop.io.Text.DEFAULT_MAX_LEN));
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
		public virtual int compareTo(object o)
		{
			org.apache.hadoop.fs.FileStatus other = (org.apache.hadoop.fs.FileStatus)o;
			return this.getPath().compareTo(other.getPath());
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
			if (!(o is org.apache.hadoop.fs.FileStatus))
			{
				return false;
			}
			org.apache.hadoop.fs.FileStatus other = (org.apache.hadoop.fs.FileStatus)o;
			return this.getPath().Equals(other.getPath());
		}

		/// <summary>
		/// Returns a hash code value for the object, which is defined as
		/// the hash code of the path name.
		/// </summary>
		/// <returns>a hash code value for the path name.</returns>
		public override int GetHashCode()
		{
			return getPath().GetHashCode();
		}

		public override string ToString()
		{
			java.lang.StringBuilder sb = new java.lang.StringBuilder();
			sb.Append(Sharpen.Runtime.getClassForObject(this).getSimpleName());
			sb.Append("{");
			sb.Append("path=" + path);
			sb.Append("; isDirectory=" + isdir);
			if (!isDirectory())
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
			sb.Append("; isSymlink=" + isSymlink());
			if (isSymlink())
			{
				sb.Append("; symlink=" + symlink);
			}
			sb.Append("}");
			return sb.ToString();
		}
	}
}
