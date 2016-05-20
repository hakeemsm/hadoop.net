using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Implement the FileSystem API for the raw local filesystem.</summary>
	public class RawLocalFileSystem : org.apache.hadoop.fs.FileSystem
	{
		internal static readonly java.net.URI NAME = java.net.URI.create("file:///");

		private org.apache.hadoop.fs.Path workingDir;

		private static bool useDeprecatedFileStatus = true;

		// Temporary workaround for HADOOP-9652.
		[com.google.common.annotations.VisibleForTesting]
		public static void useStatIfAvailable()
		{
			useDeprecatedFileStatus = !org.apache.hadoop.fs.Stat.isAvailable();
		}

		public RawLocalFileSystem()
		{
			workingDir = getInitialWorkingDirectory();
		}

		private org.apache.hadoop.fs.Path makeAbsolute(org.apache.hadoop.fs.Path f)
		{
			if (f.isAbsolute())
			{
				return f;
			}
			else
			{
				return new org.apache.hadoop.fs.Path(workingDir, f);
			}
		}

		/// <summary>Convert a path to a File.</summary>
		public virtual java.io.File pathToFile(org.apache.hadoop.fs.Path path)
		{
			checkPath(path);
			if (!path.isAbsolute())
			{
				path = new org.apache.hadoop.fs.Path(getWorkingDirectory(), path);
			}
			return new java.io.File(path.toUri().getPath());
		}

		public override java.net.URI getUri()
		{
			return NAME;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void initialize(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf)
		{
			base.initialize(uri, conf);
			setConf(conf);
		}

		/// <summary>For open()'s FSInputStream.</summary>
		internal class LocalFSFileInputStream : org.apache.hadoop.fs.FSInputStream, org.apache.hadoop.fs.HasFileDescriptor
		{
			private java.io.FileInputStream fis;

			private long position;

			/// <exception cref="System.IO.IOException"/>
			public LocalFSFileInputStream(RawLocalFileSystem _enclosing, org.apache.hadoop.fs.Path
				 f)
			{
				this._enclosing = _enclosing;
				this.fis = new java.io.FileInputStream(this._enclosing.pathToFile(f));
			}

			/// <exception cref="System.IO.IOException"/>
			public override void seek(long pos)
			{
				if (pos < 0)
				{
					throw new java.io.EOFException(org.apache.hadoop.fs.FSExceptionMessages.NEGATIVE_SEEK
						);
				}
				this.fis.getChannel().position(pos);
				this.position = pos;
			}

			/// <exception cref="System.IO.IOException"/>
			public override long getPos()
			{
				return this.position;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool seekToNewSource(long targetPos)
			{
				return false;
			}

			/*
			* Just forward to the fis
			*/
			/// <exception cref="System.IO.IOException"/>
			public override int available()
			{
				return this.fis.available();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				this.fis.close();
			}

			public override bool markSupported()
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int read()
			{
				try
				{
					int value = this.fis.read();
					if (value >= 0)
					{
						this.position++;
						this._enclosing.statistics.incrementBytesRead(1);
					}
					return value;
				}
				catch (System.IO.IOException e)
				{
					// unexpected exception
					throw new org.apache.hadoop.fs.FSError(e);
				}
			}

			// assume native fs error
			/// <exception cref="System.IO.IOException"/>
			public override int read(byte[] b, int off, int len)
			{
				try
				{
					int value = this.fis.read(b, off, len);
					if (value > 0)
					{
						this.position += value;
						this._enclosing.statistics.incrementBytesRead(value);
					}
					return value;
				}
				catch (System.IO.IOException e)
				{
					// unexpected exception
					throw new org.apache.hadoop.fs.FSError(e);
				}
			}

			// assume native fs error
			/// <exception cref="System.IO.IOException"/>
			public override int read(long position, byte[] b, int off, int len)
			{
				java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(b, off, len);
				try
				{
					int value = this.fis.getChannel().read(bb, position);
					if (value > 0)
					{
						this._enclosing.statistics.incrementBytesRead(value);
					}
					return value;
				}
				catch (System.IO.IOException e)
				{
					throw new org.apache.hadoop.fs.FSError(e);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override long skip(long n)
			{
				long value = this.fis.skip(n);
				if (value > 0)
				{
					this.position += value;
				}
				return value;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual java.io.FileDescriptor getFileDescriptor()
			{
				return this.fis.getFD();
			}

			private readonly RawLocalFileSystem _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f, int bufferSize)
		{
			if (!exists(f))
			{
				throw new java.io.FileNotFoundException(f.ToString());
			}
			return new org.apache.hadoop.fs.FSDataInputStream(new org.apache.hadoop.fs.BufferedFSInputStream
				(new org.apache.hadoop.fs.RawLocalFileSystem.LocalFSFileInputStream(this, f), bufferSize
				));
		}

		/// <summary>For create()'s FSOutputStream.</summary>
		internal class LocalFSFileOutputStream : java.io.OutputStream
		{
			private java.io.FileOutputStream fos;

			/// <exception cref="System.IO.IOException"/>
			private LocalFSFileOutputStream(RawLocalFileSystem _enclosing, org.apache.hadoop.fs.Path
				 f, bool append, org.apache.hadoop.fs.permission.FsPermission permission)
			{
				this._enclosing = _enclosing;
				java.io.File file = this._enclosing.pathToFile(f);
				if (permission == null)
				{
					this.fos = new java.io.FileOutputStream(file, append);
				}
				else
				{
					if (org.apache.hadoop.util.Shell.WINDOWS && org.apache.hadoop.io.nativeio.NativeIO
						.isAvailable())
					{
						this.fos = org.apache.hadoop.io.nativeio.NativeIO.Windows.createFileOutputStreamWithMode
							(file, append, permission.toShort());
					}
					else
					{
						this.fos = new java.io.FileOutputStream(file, append);
						bool success = false;
						try
						{
							this._enclosing.setPermission(f, permission);
							success = true;
						}
						finally
						{
							if (!success)
							{
								org.apache.hadoop.io.IOUtils.cleanup(org.apache.hadoop.fs.FileSystem.LOG, this.fos
									);
							}
						}
					}
				}
			}

			/*
			* Just forward to the fos
			*/
			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				this.fos.close();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void flush()
			{
				this.fos.flush();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(byte[] b, int off, int len)
			{
				try
				{
					this.fos.write(b, off, len);
				}
				catch (System.IO.IOException e)
				{
					// unexpected exception
					throw new org.apache.hadoop.fs.FSError(e);
				}
			}

			// assume native fs error
			/// <exception cref="System.IO.IOException"/>
			public override void write(int b)
			{
				try
				{
					this.fos.write(b);
				}
				catch (System.IO.IOException e)
				{
					// unexpected exception
					throw new org.apache.hadoop.fs.FSError(e);
				}
			}

			private readonly RawLocalFileSystem _enclosing;
			// assume native fs error
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path
			 f, int bufferSize, org.apache.hadoop.util.Progressable progress)
		{
			if (!exists(f))
			{
				throw new java.io.FileNotFoundException("File " + f + " not found");
			}
			if (getFileStatus(f).isDirectory())
			{
				throw new System.IO.IOException("Cannot append to a diretory (=" + f + " )");
			}
			return new org.apache.hadoop.fs.FSDataOutputStream(new java.io.BufferedOutputStream
				(createOutputStreamWithMode(f, true, null), bufferSize), statistics);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, bool overwrite, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			return create(f, overwrite, true, bufferSize, replication, blockSize, progress, null
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path 
			f, bool overwrite, bool createParent, int bufferSize, short replication, long blockSize
			, org.apache.hadoop.util.Progressable progress, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			if (exists(f) && !overwrite)
			{
				throw new org.apache.hadoop.fs.FileAlreadyExistsException("File already exists: "
					 + f);
			}
			org.apache.hadoop.fs.Path parent = f.getParent();
			if (parent != null && !mkdirs(parent))
			{
				throw new System.IO.IOException("Mkdirs failed to create " + parent.ToString());
			}
			return new org.apache.hadoop.fs.FSDataOutputStream(new java.io.BufferedOutputStream
				(createOutputStreamWithMode(f, false, permission), bufferSize), statistics);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual java.io.OutputStream createOutputStream(org.apache.hadoop.fs.Path
			 f, bool append)
		{
			return createOutputStreamWithMode(f, append, null);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual java.io.OutputStream createOutputStreamWithMode(org.apache.hadoop.fs.Path
			 f, bool append, org.apache.hadoop.fs.permission.FsPermission permission)
		{
			return new org.apache.hadoop.fs.RawLocalFileSystem.LocalFSFileOutputStream(this, 
				f, append, permission);
		}

		/// <exception cref="System.IO.IOException"/>
		[System.Obsolete]
		public override org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag
			> flags, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			if (exists(f) && !flags.contains(org.apache.hadoop.fs.CreateFlag.OVERWRITE))
			{
				throw new org.apache.hadoop.fs.FileAlreadyExistsException("File already exists: "
					 + f);
			}
			return new org.apache.hadoop.fs.FSDataOutputStream(new java.io.BufferedOutputStream
				(createOutputStreamWithMode(f, false, permission), bufferSize), statistics);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, int
			 bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = create(f, overwrite, true, bufferSize
				, replication, blockSize, progress, permission);
			return @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, int
			 bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = create(f, overwrite, false, bufferSize
				, replication, blockSize, progress, permission);
			return @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			// Attempt rename using Java API.
			java.io.File srcFile = pathToFile(src);
			java.io.File dstFile = pathToFile(dst);
			if (srcFile.renameTo(dstFile))
			{
				return true;
			}
			// Enforce POSIX rename behavior that a source directory replaces an existing
			// destination if the destination is an empty directory.  On most platforms,
			// this is already handled by the Java API call above.  Some platforms
			// (notably Windows) do not provide this behavior, so the Java API call above
			// fails.  Delete destination and attempt rename again.
			if (this.exists(dst))
			{
				org.apache.hadoop.fs.FileStatus sdst = this.getFileStatus(dst);
				if (sdst.isDirectory() && dstFile.list().Length == 0)
				{
					if (LOG.isDebugEnabled())
					{
						LOG.debug("Deleting empty destination and renaming " + src + " to " + dst);
					}
					if (this.delete(dst, false) && srcFile.renameTo(dstFile))
					{
						return true;
					}
				}
			}
			// The fallback behavior accomplishes the rename by a full copy.
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Falling through to a copy of " + src + " to " + dst);
			}
			return org.apache.hadoop.fs.FileUtil.copy(this, src, this, dst, true, getConf());
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool truncate(org.apache.hadoop.fs.Path f, long newLength)
		{
			org.apache.hadoop.fs.FileStatus status = getFileStatus(f);
			if (status == null)
			{
				throw new java.io.FileNotFoundException("File " + f + " not found");
			}
			if (status.isDirectory())
			{
				throw new System.IO.IOException("Cannot truncate a directory (=" + f + ")");
			}
			long oldLength = status.getLen();
			if (newLength > oldLength)
			{
				throw new System.ArgumentException("Cannot truncate to a larger file size. Current size: "
					 + oldLength + ", truncate size: " + newLength + ".");
			}
			using (java.io.FileOutputStream @out = new java.io.FileOutputStream(pathToFile(f)
				, true))
			{
				try
				{
					@out.getChannel().truncate(newLength);
				}
				catch (System.IO.IOException e)
				{
					throw new org.apache.hadoop.fs.FSError(e);
				}
			}
			return true;
		}

		/// <summary>Delete the given path to a file or directory.</summary>
		/// <param name="p">the path to delete</param>
		/// <param name="recursive">to delete sub-directories</param>
		/// <returns>true if the file or directory and all its contents were deleted</returns>
		/// <exception cref="System.IO.IOException">if p is non-empty and recursive is false</exception>
		public override bool delete(org.apache.hadoop.fs.Path p, bool recursive)
		{
			java.io.File f = pathToFile(p);
			if (!f.exists())
			{
				//no path, return false "nothing to delete"
				return false;
			}
			if (f.isFile())
			{
				return f.delete();
			}
			else
			{
				if (!recursive && f.isDirectory() && (org.apache.hadoop.fs.FileUtil.listFiles(f).
					Length != 0))
				{
					throw new System.IO.IOException("Directory " + f.ToString() + " is not empty");
				}
			}
			return org.apache.hadoop.fs.FileUtil.fullyDelete(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 f)
		{
			java.io.File localf = pathToFile(f);
			org.apache.hadoop.fs.FileStatus[] results;
			if (!localf.exists())
			{
				throw new java.io.FileNotFoundException("File " + f + " does not exist");
			}
			if (localf.isDirectory())
			{
				string[] names = localf.list();
				if (names == null)
				{
					return null;
				}
				results = new org.apache.hadoop.fs.FileStatus[names.Length];
				int j = 0;
				for (int i = 0; i < names.Length; i++)
				{
					try
					{
						// Assemble the path using the Path 3 arg constructor to make sure
						// paths with colon are properly resolved on Linux
						results[j] = getFileStatus(new org.apache.hadoop.fs.Path(f, new org.apache.hadoop.fs.Path
							(null, null, names[i])));
						j++;
					}
					catch (java.io.FileNotFoundException)
					{
					}
				}
				// ignore the files not found since the dir list may have have
				// changed since the names[] list was generated.
				if (j == names.Length)
				{
					return results;
				}
				return java.util.Arrays.copyOf(results, j);
			}
			if (!useDeprecatedFileStatus)
			{
				return new org.apache.hadoop.fs.FileStatus[] { getFileStatus(f) };
			}
			return new org.apache.hadoop.fs.FileStatus[] { new org.apache.hadoop.fs.RawLocalFileSystem.DeprecatedRawLocalFileStatus
				(localf, getDefaultBlockSize(f), this) };
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual bool mkOneDir(java.io.File p2f)
		{
			return mkOneDirWithMode(new org.apache.hadoop.fs.Path(p2f.getAbsolutePath()), p2f
				, null);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual bool mkOneDirWithMode(org.apache.hadoop.fs.Path p, java.io.File
			 p2f, org.apache.hadoop.fs.permission.FsPermission permission)
		{
			if (permission == null)
			{
				return p2f.mkdir();
			}
			else
			{
				if (org.apache.hadoop.util.Shell.WINDOWS && org.apache.hadoop.io.nativeio.NativeIO
					.isAvailable())
				{
					try
					{
						org.apache.hadoop.io.nativeio.NativeIO.Windows.createDirectoryWithMode(p2f, permission
							.toShort());
						return true;
					}
					catch (System.IO.IOException e)
					{
						if (LOG.isDebugEnabled())
						{
							LOG.debug(string.format("NativeIO.createDirectoryWithMode error, path = %s, mode = %o"
								, p2f, permission.toShort()), e);
						}
						return false;
					}
				}
				else
				{
					bool b = p2f.mkdir();
					if (b)
					{
						setPermission(p, permission);
					}
					return b;
				}
			}
		}

		/// <summary>Creates the specified directory hierarchy.</summary>
		/// <remarks>
		/// Creates the specified directory hierarchy. Does not
		/// treat existence as an error.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override bool mkdirs(org.apache.hadoop.fs.Path f)
		{
			return mkdirsWithOptionalPermission(f, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool mkdirs(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			return mkdirsWithOptionalPermission(f, permission);
		}

		/// <exception cref="System.IO.IOException"/>
		private bool mkdirsWithOptionalPermission(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			if (f == null)
			{
				throw new System.ArgumentException("mkdirs path arg is null");
			}
			org.apache.hadoop.fs.Path parent = f.getParent();
			java.io.File p2f = pathToFile(f);
			java.io.File parent2f = null;
			if (parent != null)
			{
				parent2f = pathToFile(parent);
				if (parent2f != null && parent2f.exists() && !parent2f.isDirectory())
				{
					throw new org.apache.hadoop.fs.ParentNotDirectoryException("Parent path is not a directory: "
						 + parent);
				}
			}
			if (p2f.exists() && !p2f.isDirectory())
			{
				throw new java.io.FileNotFoundException("Destination exists" + " and is not a directory: "
					 + p2f.getCanonicalPath());
			}
			return (parent == null || parent2f.exists() || mkdirs(parent)) && (mkOneDirWithMode
				(f, p2f, permission) || p2f.isDirectory());
		}

		public override org.apache.hadoop.fs.Path getHomeDirectory()
		{
			return this.makeQualified(new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty
				("user.home")));
		}

		/// <summary>Set the working directory to the given directory.</summary>
		public override void setWorkingDirectory(org.apache.hadoop.fs.Path newDir)
		{
			workingDir = makeAbsolute(newDir);
			checkPath(workingDir);
		}

		public override org.apache.hadoop.fs.Path getWorkingDirectory()
		{
			return workingDir;
		}

		protected internal override org.apache.hadoop.fs.Path getInitialWorkingDirectory(
			)
		{
			return this.makeQualified(new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty
				("user.dir")));
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsStatus getStatus(org.apache.hadoop.fs.Path
			 p)
		{
			java.io.File partition = pathToFile(p == null ? new org.apache.hadoop.fs.Path("/"
				) : p);
			//File provides getUsableSpace() and getFreeSpace()
			//File provides no API to obtain used space, assume used = total - free
			return new org.apache.hadoop.fs.FsStatus(partition.getTotalSpace(), partition.getTotalSpace
				() - partition.getFreeSpace(), partition.getFreeSpace());
		}

		// In the case of the local filesystem, we can just rename the file.
		/// <exception cref="System.IO.IOException"/>
		public override void moveFromLocalFile(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			rename(src, dst);
		}

		// We can write output directly to the final location
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path startLocalOutput(org.apache.hadoop.fs.Path
			 fsOutputFile, org.apache.hadoop.fs.Path tmpLocalFile)
		{
			return fsOutputFile;
		}

		// It's in the right place - nothing to do.
		/// <exception cref="System.IO.IOException"/>
		public override void completeLocalOutput(org.apache.hadoop.fs.Path fsWorkingFile, 
			org.apache.hadoop.fs.Path tmpLocalFile)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			base.close();
		}

		public override string ToString()
		{
			return "LocalFS";
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return getFileLinkStatusInternal(f, true);
		}

		/// <exception cref="System.IO.IOException"/>
		[System.Obsolete]
		private org.apache.hadoop.fs.FileStatus deprecatedGetFileStatus(org.apache.hadoop.fs.Path
			 f)
		{
			java.io.File path = pathToFile(f);
			if (path.exists())
			{
				return new org.apache.hadoop.fs.RawLocalFileSystem.DeprecatedRawLocalFileStatus(pathToFile
					(f), getDefaultBlockSize(f), this);
			}
			else
			{
				throw new java.io.FileNotFoundException("File " + f + " does not exist");
			}
		}

		internal class DeprecatedRawLocalFileStatus : org.apache.hadoop.fs.FileStatus
		{
			/* We can add extra fields here. It breaks at least CopyFiles.FilePair().
			* We recognize if the information is already loaded by check if
			* onwer.equals("").
			*/
			private bool isPermissionLoaded()
			{
				return !base.getOwner().isEmpty();
			}

			internal DeprecatedRawLocalFileStatus(java.io.File f, long defaultBlockSize, org.apache.hadoop.fs.FileSystem
				 fs)
				: base(f.length(), f.isDirectory(), 1, defaultBlockSize, f.lastModified(), new org.apache.hadoop.fs.Path
					(f.getPath()).makeQualified(fs.getUri(), fs.getWorkingDirectory()))
			{
			}

			public override org.apache.hadoop.fs.permission.FsPermission getPermission()
			{
				if (!isPermissionLoaded())
				{
					loadPermissionInfo();
				}
				return base.getPermission();
			}

			public override string getOwner()
			{
				if (!isPermissionLoaded())
				{
					loadPermissionInfo();
				}
				return base.getOwner();
			}

			public override string getGroup()
			{
				if (!isPermissionLoaded())
				{
					loadPermissionInfo();
				}
				return base.getGroup();
			}

			/// loads permissions, owner, and group from `ls -ld`
			private void loadPermissionInfo()
			{
				System.IO.IOException e = null;
				try
				{
					string output = org.apache.hadoop.fs.FileUtil.execCommand(new java.io.File(getPath
						().toUri()), org.apache.hadoop.util.Shell.getGetPermissionCommand());
					java.util.StringTokenizer t = new java.util.StringTokenizer(output, org.apache.hadoop.util.Shell
						.TOKEN_SEPARATOR_REGEX);
					//expected format
					//-rw-------    1 username groupname ...
					string permission = t.nextToken();
					if (permission.Length > org.apache.hadoop.fs.permission.FsPermission.MAX_PERMISSION_LENGTH)
					{
						//files with ACLs might have a '+'
						permission = Sharpen.Runtime.substring(permission, 0, org.apache.hadoop.fs.permission.FsPermission
							.MAX_PERMISSION_LENGTH);
					}
					setPermission(org.apache.hadoop.fs.permission.FsPermission.valueOf(permission));
					t.nextToken();
					string owner = t.nextToken();
					// If on windows domain, token format is DOMAIN\\user and we want to
					// extract only the user name
					if (org.apache.hadoop.util.Shell.WINDOWS)
					{
						int i = owner.IndexOf('\\');
						if (i != -1)
						{
							owner = Sharpen.Runtime.substring(owner, i + 1);
						}
					}
					setOwner(owner);
					setGroup(t.nextToken());
				}
				catch (org.apache.hadoop.util.Shell.ExitCodeException ioe)
				{
					if (ioe.getExitCode() != 1)
					{
						e = ioe;
					}
					else
					{
						setPermission(null);
						setOwner(null);
						setGroup(null);
					}
				}
				catch (System.IO.IOException ioe)
				{
					e = ioe;
				}
				finally
				{
					if (e != null)
					{
						throw new System.Exception("Error while running command to get " + "file permissions : "
							 + org.apache.hadoop.util.StringUtils.stringifyException(e));
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(java.io.DataOutput @out)
			{
				if (!isPermissionLoaded())
				{
					loadPermissionInfo();
				}
				base.write(@out);
			}
		}

		/// <summary>Use the command chown to set owner.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void setOwner(org.apache.hadoop.fs.Path p, string username, string
			 groupname)
		{
			org.apache.hadoop.fs.FileUtil.setOwner(pathToFile(p), username, groupname);
		}

		/// <summary>Use the command chmod to set permission.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void setPermission(org.apache.hadoop.fs.Path p, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			if (org.apache.hadoop.io.nativeio.NativeIO.isAvailable())
			{
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.chmod(pathToFile(p).getCanonicalPath
					(), permission.toShort());
			}
			else
			{
				string perm = string.format("%04o", permission.toShort());
				org.apache.hadoop.util.Shell.execCommand(org.apache.hadoop.util.Shell.getSetPermissionCommand
					(perm, false, org.apache.hadoop.fs.FileUtil.makeShellPath(pathToFile(p), true)));
			}
		}

		/// <summary>
		/// Sets the
		/// <see cref="Path"/>
		/// 's last modified time <em>only</em> to the given
		/// valid time.
		/// </summary>
		/// <param name="mtime">the modification time to set (only if greater than zero).</param>
		/// <param name="atime">currently ignored.</param>
		/// <exception cref="System.IO.IOException">if setting the last modified time fails.</exception>
		public override void setTimes(org.apache.hadoop.fs.Path p, long mtime, long atime
			)
		{
			java.io.File f = pathToFile(p);
			if (mtime >= 0)
			{
				if (!f.setLastModified(mtime))
				{
					throw new System.IO.IOException("couldn't set last-modified time to " + mtime + " for "
						 + f.getAbsolutePath());
				}
			}
		}

		public override bool supportsSymlinks()
		{
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void createSymlink(org.apache.hadoop.fs.Path target, org.apache.hadoop.fs.Path
			 link, bool createParent)
		{
			if (!org.apache.hadoop.fs.FileSystem.areSymlinksEnabled())
			{
				throw new System.NotSupportedException("Symlinks not supported");
			}
			string targetScheme = target.toUri().getScheme();
			if (targetScheme != null && !"file".Equals(targetScheme))
			{
				throw new System.IO.IOException("Unable to create symlink to non-local file " + "system: "
					 + target.ToString());
			}
			if (createParent)
			{
				mkdirs(link.getParent());
			}
			// NB: Use createSymbolicLink in java.nio.file.Path once available
			int result = org.apache.hadoop.fs.FileUtil.symLink(target.ToString(), makeAbsolute
				(link).ToString());
			if (result != 0)
			{
				throw new System.IO.IOException("Error " + result + " creating symlink " + link +
					 " to " + target);
			}
		}

		/// <summary>Return a FileStatus representing the given path.</summary>
		/// <remarks>
		/// Return a FileStatus representing the given path. If the path refers
		/// to a symlink return a FileStatus representing the link rather than
		/// the object the link refers to.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.FileStatus fi = getFileLinkStatusInternal(f, false);
			// getFileLinkStatus is supposed to return a symlink with a
			// qualified path
			if (fi.isSymlink())
			{
				org.apache.hadoop.fs.Path targetQual = org.apache.hadoop.fs.FSLinkResolver.qualifySymlinkTarget
					(this.getUri(), fi.getPath(), fi.getSymlink());
				fi.setSymlink(targetQual);
			}
			return fi;
		}

		/// <summary>
		/// Public
		/// <see cref="FileStatus"/>
		/// methods delegate to this function, which in turn
		/// either call the new
		/// <see cref="Stat"/>
		/// based implementation or the deprecated
		/// methods based on platform support.
		/// </summary>
		/// <param name="f">Path to stat</param>
		/// <param name="dereference">
		/// whether to dereference the final path component if a
		/// symlink
		/// </param>
		/// <returns>FileStatus of f</returns>
		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.fs.FileStatus getFileLinkStatusInternal(org.apache.hadoop.fs.Path
			 f, bool dereference)
		{
			if (!useDeprecatedFileStatus)
			{
				return getNativeFileLinkStatus(f, dereference);
			}
			else
			{
				if (dereference)
				{
					return deprecatedGetFileStatus(f);
				}
				else
				{
					return deprecatedGetFileLinkStatusInternal(f);
				}
			}
		}

		/// <summary>Deprecated.</summary>
		/// <remarks>
		/// Deprecated. Remains for legacy support. Should be removed when
		/// <see cref="Stat"/>
		/// gains support for Windows and other operating systems.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[System.Obsolete]
		private org.apache.hadoop.fs.FileStatus deprecatedGetFileLinkStatusInternal(org.apache.hadoop.fs.Path
			 f)
		{
			string target = org.apache.hadoop.fs.FileUtil.readLink(new java.io.File(f.ToString
				()));
			try
			{
				org.apache.hadoop.fs.FileStatus fs = getFileStatus(f);
				// If f refers to a regular file or directory
				if (target.isEmpty())
				{
					return fs;
				}
				// Otherwise f refers to a symlink
				return new org.apache.hadoop.fs.FileStatus(fs.getLen(), false, fs.getReplication(
					), fs.getBlockSize(), fs.getModificationTime(), fs.getAccessTime(), fs.getPermission
					(), fs.getOwner(), fs.getGroup(), new org.apache.hadoop.fs.Path(target), f);
			}
			catch (java.io.FileNotFoundException e)
			{
				/* The exists method in the File class returns false for dangling
				* links so we can get a FileNotFoundException for links that exist.
				* It's also possible that we raced with a delete of the link. Use
				* the readBasicFileAttributes method in java.nio.file.attributes
				* when available.
				*/
				if (!target.isEmpty())
				{
					return new org.apache.hadoop.fs.FileStatus(0, false, 0, 0, 0, 0, org.apache.hadoop.fs.permission.FsPermission
						.getDefault(), string.Empty, string.Empty, new org.apache.hadoop.fs.Path(target)
						, f);
				}
				// f refers to a file or directory that does not exist
				throw;
			}
		}

		/// <summary>
		/// Calls out to platform's native stat(1) implementation to get file metadata
		/// (permissions, user, group, atime, mtime, etc).
		/// </summary>
		/// <remarks>
		/// Calls out to platform's native stat(1) implementation to get file metadata
		/// (permissions, user, group, atime, mtime, etc). This works around the lack
		/// of lstat(2) in Java 6.
		/// Currently, the
		/// <see cref="Stat"/>
		/// class used to do this only supports Linux
		/// and FreeBSD, so the old
		/// <see cref="deprecatedGetFileLinkStatusInternal(Path)"/>
		/// implementation (deprecated) remains further OS support is added.
		/// </remarks>
		/// <param name="f">File to stat</param>
		/// <param name="dereference">whether to dereference symlinks</param>
		/// <returns>FileStatus of f</returns>
		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.fs.FileStatus getNativeFileLinkStatus(org.apache.hadoop.fs.Path
			 f, bool dereference)
		{
			checkPath(f);
			org.apache.hadoop.fs.Stat stat = new org.apache.hadoop.fs.Stat(f, getDefaultBlockSize
				(f), dereference, this);
			org.apache.hadoop.fs.FileStatus status = stat.getFileStatus();
			return status;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path
			 f)
		{
			org.apache.hadoop.fs.FileStatus fi = getFileLinkStatusInternal(f, false);
			// return an unqualified symlink target
			return fi.getSymlink();
		}
	}
}
