using System;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Implement the FileSystem API for the raw local filesystem.</summary>
	public class RawLocalFileSystem : FileSystem
	{
		internal static readonly URI Name = URI.Create("file:///");

		private Path workingDir;

		private static bool useDeprecatedFileStatus = true;

		// Temporary workaround for HADOOP-9652.
		[VisibleForTesting]
		public static void UseStatIfAvailable()
		{
			useDeprecatedFileStatus = !Stat.IsAvailable();
		}

		public RawLocalFileSystem()
		{
			workingDir = GetInitialWorkingDirectory();
		}

		private Path MakeAbsolute(Path f)
		{
			if (f.IsAbsolute())
			{
				return f;
			}
			else
			{
				return new Path(workingDir, f);
			}
		}

		/// <summary>Convert a path to a File.</summary>
		public virtual FilePath PathToFile(Path path)
		{
			CheckPath(path);
			if (!path.IsAbsolute())
			{
				path = new Path(GetWorkingDirectory(), path);
			}
			return new FilePath(path.ToUri().GetPath());
		}

		public override URI GetUri()
		{
			return Name;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Initialize(URI uri, Configuration conf)
		{
			base.Initialize(uri, conf);
			SetConf(conf);
		}

		/// <summary>For open()'s FSInputStream.</summary>
		internal class LocalFSFileInputStream : FSInputStream, HasFileDescriptor
		{
			private FileInputStream fis;

			private long position;

			/// <exception cref="System.IO.IOException"/>
			public LocalFSFileInputStream(RawLocalFileSystem _enclosing, Path f)
			{
				this._enclosing = _enclosing;
				this.fis = new FileInputStream(this._enclosing.PathToFile(f));
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Seek(long pos)
			{
				if (pos < 0)
				{
					throw new EOFException(FSExceptionMessages.NegativeSeek);
				}
				this.fis.GetChannel().Position(pos);
				this.position = pos;
			}

			/// <exception cref="System.IO.IOException"/>
			public override long GetPos()
			{
				return this.position;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool SeekToNewSource(long targetPos)
			{
				return false;
			}

			/*
			* Just forward to the fis
			*/
			/// <exception cref="System.IO.IOException"/>
			public override int Available()
			{
				return this.fis.Available();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				this.fis.Close();
			}

			public override bool MarkSupported()
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read()
			{
				try
				{
					int value = this.fis.Read();
					if (value >= 0)
					{
						this.position++;
						this._enclosing.statistics.IncrementBytesRead(1);
					}
					return value;
				}
				catch (IOException e)
				{
					// unexpected exception
					throw new FSError(e);
				}
			}

			// assume native fs error
			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] b, int off, int len)
			{
				try
				{
					int value = this.fis.Read(b, off, len);
					if (value > 0)
					{
						this.position += value;
						this._enclosing.statistics.IncrementBytesRead(value);
					}
					return value;
				}
				catch (IOException e)
				{
					// unexpected exception
					throw new FSError(e);
				}
			}

			// assume native fs error
			/// <exception cref="System.IO.IOException"/>
			public override int Read(long position, byte[] b, int off, int len)
			{
				ByteBuffer bb = ByteBuffer.Wrap(b, off, len);
				try
				{
					int value = this.fis.GetChannel().Read(bb, position);
					if (value > 0)
					{
						this._enclosing.statistics.IncrementBytesRead(value);
					}
					return value;
				}
				catch (IOException e)
				{
					throw new FSError(e);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override long Skip(long n)
			{
				long value = this.fis.Skip(n);
				if (value > 0)
				{
					this.position += value;
				}
				return value;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FileDescriptor GetFileDescriptor()
			{
				return this.fis.GetFD();
			}

			private readonly RawLocalFileSystem _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataInputStream Open(Path f, int bufferSize)
		{
			if (!Exists(f))
			{
				throw new FileNotFoundException(f.ToString());
			}
			return new FSDataInputStream(new BufferedFSInputStream(new RawLocalFileSystem.LocalFSFileInputStream
				(this, f), bufferSize));
		}

		/// <summary>For create()'s FSOutputStream.</summary>
		internal class LocalFSFileOutputStream : OutputStream
		{
			private FileOutputStream fos;

			/// <exception cref="System.IO.IOException"/>
			private LocalFSFileOutputStream(RawLocalFileSystem _enclosing, Path f, bool append
				, FsPermission permission)
			{
				this._enclosing = _enclosing;
				FilePath file = this._enclosing.PathToFile(f);
				if (permission == null)
				{
					this.fos = new FileOutputStream(file, append);
				}
				else
				{
					if (Shell.Windows && NativeIO.IsAvailable())
					{
						this.fos = NativeIO.Windows.CreateFileOutputStreamWithMode(file, append, permission
							.ToShort());
					}
					else
					{
						this.fos = new FileOutputStream(file, append);
						bool success = false;
						try
						{
							this._enclosing.SetPermission(f, permission);
							success = true;
						}
						finally
						{
							if (!success)
							{
								IOUtils.Cleanup(FileSystem.Log, this.fos);
							}
						}
					}
				}
			}

			/*
			* Just forward to the fos
			*/
			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				this.fos.Close();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Flush()
			{
				this.fos.Flush();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(byte[] b, int off, int len)
			{
				try
				{
					this.fos.Write(b, off, len);
				}
				catch (IOException e)
				{
					// unexpected exception
					throw new FSError(e);
				}
			}

			// assume native fs error
			/// <exception cref="System.IO.IOException"/>
			public override void Write(int b)
			{
				try
				{
					this.fos.Write(b);
				}
				catch (IOException e)
				{
					// unexpected exception
					throw new FSError(e);
				}
			}

			private readonly RawLocalFileSystem _enclosing;
			// assume native fs error
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Append(Path f, int bufferSize, Progressable progress
			)
		{
			if (!Exists(f))
			{
				throw new FileNotFoundException("File " + f + " not found");
			}
			if (GetFileStatus(f).IsDirectory())
			{
				throw new IOException("Cannot append to a diretory (=" + f + " )");
			}
			return new FSDataOutputStream(new BufferedOutputStream(CreateOutputStreamWithMode
				(f, true, null), bufferSize), statistics);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Create(Path f, bool overwrite, int bufferSize, 
			short replication, long blockSize, Progressable progress)
		{
			return Create(f, overwrite, true, bufferSize, replication, blockSize, progress, null
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private FSDataOutputStream Create(Path f, bool overwrite, bool createParent, int 
			bufferSize, short replication, long blockSize, Progressable progress, FsPermission
			 permission)
		{
			if (Exists(f) && !overwrite)
			{
				throw new FileAlreadyExistsException("File already exists: " + f);
			}
			Path parent = f.GetParent();
			if (parent != null && !Mkdirs(parent))
			{
				throw new IOException("Mkdirs failed to create " + parent.ToString());
			}
			return new FSDataOutputStream(new BufferedOutputStream(CreateOutputStreamWithMode
				(f, false, permission), bufferSize), statistics);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual OutputStream CreateOutputStream(Path f, bool append)
		{
			return CreateOutputStreamWithMode(f, append, null);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual OutputStream CreateOutputStreamWithMode(Path f, bool append
			, FsPermission permission)
		{
			return new RawLocalFileSystem.LocalFSFileOutputStream(this, f, append, permission
				);
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public override FSDataOutputStream CreateNonRecursive(Path f, FsPermission permission
			, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, 
			Progressable progress)
		{
			if (Exists(f) && !flags.Contains(CreateFlag.Overwrite))
			{
				throw new FileAlreadyExistsException("File already exists: " + f);
			}
			return new FSDataOutputStream(new BufferedOutputStream(CreateOutputStreamWithMode
				(f, false, permission), bufferSize), statistics);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
			, int bufferSize, short replication, long blockSize, Progressable progress)
		{
			FSDataOutputStream @out = Create(f, overwrite, true, bufferSize, replication, blockSize
				, progress, permission);
			return @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream CreateNonRecursive(Path f, FsPermission permission
			, bool overwrite, int bufferSize, short replication, long blockSize, Progressable
			 progress)
		{
			FSDataOutputStream @out = Create(f, overwrite, false, bufferSize, replication, blockSize
				, progress, permission);
			return @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Rename(Path src, Path dst)
		{
			// Attempt rename using Java API.
			FilePath srcFile = PathToFile(src);
			FilePath dstFile = PathToFile(dst);
			if (srcFile.RenameTo(dstFile))
			{
				return true;
			}
			// Enforce POSIX rename behavior that a source directory replaces an existing
			// destination if the destination is an empty directory.  On most platforms,
			// this is already handled by the Java API call above.  Some platforms
			// (notably Windows) do not provide this behavior, so the Java API call above
			// fails.  Delete destination and attempt rename again.
			if (this.Exists(dst))
			{
				FileStatus sdst = this.GetFileStatus(dst);
				if (sdst.IsDirectory() && dstFile.List().Length == 0)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Deleting empty destination and renaming " + src + " to " + dst);
					}
					if (this.Delete(dst, false) && srcFile.RenameTo(dstFile))
					{
						return true;
					}
				}
			}
			// The fallback behavior accomplishes the rename by a full copy.
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Falling through to a copy of " + src + " to " + dst);
			}
			return FileUtil.Copy(this, src, this, dst, true, GetConf());
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Truncate(Path f, long newLength)
		{
			FileStatus status = GetFileStatus(f);
			if (status == null)
			{
				throw new FileNotFoundException("File " + f + " not found");
			}
			if (status.IsDirectory())
			{
				throw new IOException("Cannot truncate a directory (=" + f + ")");
			}
			long oldLength = status.GetLen();
			if (newLength > oldLength)
			{
				throw new ArgumentException("Cannot truncate to a larger file size. Current size: "
					 + oldLength + ", truncate size: " + newLength + ".");
			}
			using (FileOutputStream @out = new FileOutputStream(PathToFile(f), true))
			{
				try
				{
					@out.GetChannel().Truncate(newLength);
				}
				catch (IOException e)
				{
					throw new FSError(e);
				}
			}
			return true;
		}

		/// <summary>Delete the given path to a file or directory.</summary>
		/// <param name="p">the path to delete</param>
		/// <param name="recursive">to delete sub-directories</param>
		/// <returns>true if the file or directory and all its contents were deleted</returns>
		/// <exception cref="System.IO.IOException">if p is non-empty and recursive is false</exception>
		public override bool Delete(Path p, bool recursive)
		{
			FilePath f = PathToFile(p);
			if (!f.Exists())
			{
				//no path, return false "nothing to delete"
				return false;
			}
			if (f.IsFile())
			{
				return f.Delete();
			}
			else
			{
				if (!recursive && f.IsDirectory() && (FileUtil.ListFiles(f).Length != 0))
				{
					throw new IOException("Directory " + f.ToString() + " is not empty");
				}
			}
			return FileUtil.FullyDelete(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			FilePath localf = PathToFile(f);
			FileStatus[] results;
			if (!localf.Exists())
			{
				throw new FileNotFoundException("File " + f + " does not exist");
			}
			if (localf.IsDirectory())
			{
				string[] names = localf.List();
				if (names == null)
				{
					return null;
				}
				results = new FileStatus[names.Length];
				int j = 0;
				for (int i = 0; i < names.Length; i++)
				{
					try
					{
						// Assemble the path using the Path 3 arg constructor to make sure
						// paths with colon are properly resolved on Linux
						results[j] = GetFileStatus(new Path(f, new Path(null, null, names[i])));
						j++;
					}
					catch (FileNotFoundException)
					{
					}
				}
				// ignore the files not found since the dir list may have have
				// changed since the names[] list was generated.
				if (j == names.Length)
				{
					return results;
				}
				return Arrays.CopyOf(results, j);
			}
			if (!useDeprecatedFileStatus)
			{
				return new FileStatus[] { GetFileStatus(f) };
			}
			return new FileStatus[] { new RawLocalFileSystem.DeprecatedRawLocalFileStatus(localf
				, GetDefaultBlockSize(f), this) };
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual bool MkOneDir(FilePath p2f)
		{
			return MkOneDirWithMode(new Path(p2f.GetAbsolutePath()), p2f, null);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual bool MkOneDirWithMode(Path p, FilePath p2f, FsPermission
			 permission)
		{
			if (permission == null)
			{
				return p2f.Mkdir();
			}
			else
			{
				if (Shell.Windows && NativeIO.IsAvailable())
				{
					try
					{
						NativeIO.Windows.CreateDirectoryWithMode(p2f, permission.ToShort());
						return true;
					}
					catch (IOException e)
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug(string.Format("NativeIO.createDirectoryWithMode error, path = %s, mode = %o"
								, p2f, permission.ToShort()), e);
						}
						return false;
					}
				}
				else
				{
					bool b = p2f.Mkdir();
					if (b)
					{
						SetPermission(p, permission);
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
		public override bool Mkdirs(Path f)
		{
			return MkdirsWithOptionalPermission(f, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Mkdirs(Path f, FsPermission permission)
		{
			return MkdirsWithOptionalPermission(f, permission);
		}

		/// <exception cref="System.IO.IOException"/>
		private bool MkdirsWithOptionalPermission(Path f, FsPermission permission)
		{
			if (f == null)
			{
				throw new ArgumentException("mkdirs path arg is null");
			}
			Path parent = f.GetParent();
			FilePath p2f = PathToFile(f);
			FilePath parent2f = null;
			if (parent != null)
			{
				parent2f = PathToFile(parent);
				if (parent2f != null && parent2f.Exists() && !parent2f.IsDirectory())
				{
					throw new ParentNotDirectoryException("Parent path is not a directory: " + parent
						);
				}
			}
			if (p2f.Exists() && !p2f.IsDirectory())
			{
				throw new FileNotFoundException("Destination exists" + " and is not a directory: "
					 + p2f.GetCanonicalPath());
			}
			return (parent == null || parent2f.Exists() || Mkdirs(parent)) && (MkOneDirWithMode
				(f, p2f, permission) || p2f.IsDirectory());
		}

		public override Path GetHomeDirectory()
		{
			return this.MakeQualified(new Path(Runtime.GetProperty("user.home")));
		}

		/// <summary>Set the working directory to the given directory.</summary>
		public override void SetWorkingDirectory(Path newDir)
		{
			workingDir = MakeAbsolute(newDir);
			CheckPath(workingDir);
		}

		public override Path GetWorkingDirectory()
		{
			return workingDir;
		}

		protected internal override Path GetInitialWorkingDirectory()
		{
			return this.MakeQualified(new Path(Runtime.GetProperty("user.dir")));
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsStatus GetStatus(Path p)
		{
			FilePath partition = PathToFile(p == null ? new Path("/") : p);
			//File provides getUsableSpace() and getFreeSpace()
			//File provides no API to obtain used space, assume used = total - free
			return new FsStatus(partition.GetTotalSpace(), partition.GetTotalSpace() - partition
				.GetFreeSpace(), partition.GetFreeSpace());
		}

		// In the case of the local filesystem, we can just rename the file.
		/// <exception cref="System.IO.IOException"/>
		public override void MoveFromLocalFile(Path src, Path dst)
		{
			Rename(src, dst);
		}

		// We can write output directly to the final location
		/// <exception cref="System.IO.IOException"/>
		public override Path StartLocalOutput(Path fsOutputFile, Path tmpLocalFile)
		{
			return fsOutputFile;
		}

		// It's in the right place - nothing to do.
		/// <exception cref="System.IO.IOException"/>
		public override void CompleteLocalOutput(Path fsWorkingFile, Path tmpLocalFile)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			base.Close();
		}

		public override string ToString()
		{
			return "LocalFS";
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileStatus(Path f)
		{
			return GetFileLinkStatusInternal(f, true);
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		private FileStatus DeprecatedGetFileStatus(Path f)
		{
			FilePath path = PathToFile(f);
			if (path.Exists())
			{
				return new RawLocalFileSystem.DeprecatedRawLocalFileStatus(PathToFile(f), GetDefaultBlockSize
					(f), this);
			}
			else
			{
				throw new FileNotFoundException("File " + f + " does not exist");
			}
		}

		internal class DeprecatedRawLocalFileStatus : FileStatus
		{
			/* We can add extra fields here. It breaks at least CopyFiles.FilePair().
			* We recognize if the information is already loaded by check if
			* onwer.equals("").
			*/
			private bool IsPermissionLoaded()
			{
				return !base.GetOwner().IsEmpty();
			}

			internal DeprecatedRawLocalFileStatus(FilePath f, long defaultBlockSize, FileSystem
				 fs)
				: base(f.Length(), f.IsDirectory(), 1, defaultBlockSize, f.LastModified(), new Path
					(f.GetPath()).MakeQualified(fs.GetUri(), fs.GetWorkingDirectory()))
			{
			}

			public override FsPermission GetPermission()
			{
				if (!IsPermissionLoaded())
				{
					LoadPermissionInfo();
				}
				return base.GetPermission();
			}

			public override string GetOwner()
			{
				if (!IsPermissionLoaded())
				{
					LoadPermissionInfo();
				}
				return base.GetOwner();
			}

			public override string GetGroup()
			{
				if (!IsPermissionLoaded())
				{
					LoadPermissionInfo();
				}
				return base.GetGroup();
			}

			/// loads permissions, owner, and group from `ls -ld`
			private void LoadPermissionInfo()
			{
				IOException e = null;
				try
				{
					string output = FileUtil.ExecCommand(new FilePath(GetPath().ToUri()), Shell.GetGetPermissionCommand
						());
					StringTokenizer t = new StringTokenizer(output, Shell.TokenSeparatorRegex);
					//expected format
					//-rw-------    1 username groupname ...
					string permission = t.NextToken();
					if (permission.Length > FsPermission.MaxPermissionLength)
					{
						//files with ACLs might have a '+'
						permission = Sharpen.Runtime.Substring(permission, 0, FsPermission.MaxPermissionLength
							);
					}
					SetPermission(FsPermission.ValueOf(permission));
					t.NextToken();
					string owner = t.NextToken();
					// If on windows domain, token format is DOMAIN\\user and we want to
					// extract only the user name
					if (Shell.Windows)
					{
						int i = owner.IndexOf('\\');
						if (i != -1)
						{
							owner = Sharpen.Runtime.Substring(owner, i + 1);
						}
					}
					SetOwner(owner);
					SetGroup(t.NextToken());
				}
				catch (Shell.ExitCodeException ioe)
				{
					if (ioe.GetExitCode() != 1)
					{
						e = ioe;
					}
					else
					{
						SetPermission(null);
						SetOwner(null);
						SetGroup(null);
					}
				}
				catch (IOException ioe)
				{
					e = ioe;
				}
				finally
				{
					if (e != null)
					{
						throw new RuntimeException("Error while running command to get " + "file permissions : "
							 + StringUtils.StringifyException(e));
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(DataOutput @out)
			{
				if (!IsPermissionLoaded())
				{
					LoadPermissionInfo();
				}
				base.Write(@out);
			}
		}

		/// <summary>Use the command chown to set owner.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void SetOwner(Path p, string username, string groupname)
		{
			FileUtil.SetOwner(PathToFile(p), username, groupname);
		}

		/// <summary>Use the command chmod to set permission.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void SetPermission(Path p, FsPermission permission)
		{
			if (NativeIO.IsAvailable())
			{
				NativeIO.POSIX.Chmod(PathToFile(p).GetCanonicalPath(), permission.ToShort());
			}
			else
			{
				string perm = string.Format("%04o", permission.ToShort());
				Shell.ExecCommand(Shell.GetSetPermissionCommand(perm, false, FileUtil.MakeShellPath
					(PathToFile(p), true)));
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
		public override void SetTimes(Path p, long mtime, long atime)
		{
			FilePath f = PathToFile(p);
			if (mtime >= 0)
			{
				if (!f.SetLastModified(mtime))
				{
					throw new IOException("couldn't set last-modified time to " + mtime + " for " + f
						.GetAbsolutePath());
				}
			}
		}

		public override bool SupportsSymlinks()
		{
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CreateSymlink(Path target, Path link, bool createParent)
		{
			if (!FileSystem.AreSymlinksEnabled())
			{
				throw new NotSupportedException("Symlinks not supported");
			}
			string targetScheme = target.ToUri().GetScheme();
			if (targetScheme != null && !"file".Equals(targetScheme))
			{
				throw new IOException("Unable to create symlink to non-local file " + "system: " 
					+ target.ToString());
			}
			if (createParent)
			{
				Mkdirs(link.GetParent());
			}
			// NB: Use createSymbolicLink in java.nio.file.Path once available
			int result = FileUtil.SymLink(target.ToString(), MakeAbsolute(link).ToString());
			if (result != 0)
			{
				throw new IOException("Error " + result + " creating symlink " + link + " to " + 
					target);
			}
		}

		/// <summary>Return a FileStatus representing the given path.</summary>
		/// <remarks>
		/// Return a FileStatus representing the given path. If the path refers
		/// to a symlink return a FileStatus representing the link rather than
		/// the object the link refers to.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileLinkStatus(Path f)
		{
			FileStatus fi = GetFileLinkStatusInternal(f, false);
			// getFileLinkStatus is supposed to return a symlink with a
			// qualified path
			if (fi.IsSymlink())
			{
				Path targetQual = FSLinkResolver.QualifySymlinkTarget(this.GetUri(), fi.GetPath()
					, fi.GetSymlink());
				fi.SetSymlink(targetQual);
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
		private FileStatus GetFileLinkStatusInternal(Path f, bool dereference)
		{
			if (!useDeprecatedFileStatus)
			{
				return GetNativeFileLinkStatus(f, dereference);
			}
			else
			{
				if (dereference)
				{
					return DeprecatedGetFileStatus(f);
				}
				else
				{
					return DeprecatedGetFileLinkStatusInternal(f);
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
		[Obsolete]
		private FileStatus DeprecatedGetFileLinkStatusInternal(Path f)
		{
			string target = FileUtil.ReadLink(new FilePath(f.ToString()));
			try
			{
				FileStatus fs = GetFileStatus(f);
				// If f refers to a regular file or directory
				if (target.IsEmpty())
				{
					return fs;
				}
				// Otherwise f refers to a symlink
				return new FileStatus(fs.GetLen(), false, fs.GetReplication(), fs.GetBlockSize(), 
					fs.GetModificationTime(), fs.GetAccessTime(), fs.GetPermission(), fs.GetOwner(), 
					fs.GetGroup(), new Path(target), f);
			}
			catch (FileNotFoundException e)
			{
				/* The exists method in the File class returns false for dangling
				* links so we can get a FileNotFoundException for links that exist.
				* It's also possible that we raced with a delete of the link. Use
				* the readBasicFileAttributes method in java.nio.file.attributes
				* when available.
				*/
				if (!target.IsEmpty())
				{
					return new FileStatus(0, false, 0, 0, 0, 0, FsPermission.GetDefault(), string.Empty
						, string.Empty, new Path(target), f);
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
		/// <see cref="DeprecatedGetFileLinkStatusInternal(Path)"/>
		/// implementation (deprecated) remains further OS support is added.
		/// </remarks>
		/// <param name="f">File to stat</param>
		/// <param name="dereference">whether to dereference symlinks</param>
		/// <returns>FileStatus of f</returns>
		/// <exception cref="System.IO.IOException"/>
		private FileStatus GetNativeFileLinkStatus(Path f, bool dereference)
		{
			CheckPath(f);
			Stat stat = new Stat(f, GetDefaultBlockSize(f), dereference, this);
			FileStatus status = stat.GetFileStatus();
			return status;
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path GetLinkTarget(Path f)
		{
			FileStatus fi = GetFileLinkStatusInternal(f, false);
			// return an unqualified symlink target
			return fi.GetSymlink();
		}
	}
}
