using Sharpen;

namespace org.apache.hadoop.io.nativeio
{
	/// <summary>JNI wrappers for various native IO-related calls not available in Java.</summary>
	/// <remarks>
	/// JNI wrappers for various native IO-related calls not available in Java.
	/// These functions should generally be used alongside a fallback to another
	/// more portable mechanism.
	/// </remarks>
	public class NativeIO
	{
		public class POSIX
		{
			public const int O_RDONLY = 00;

			public const int O_WRONLY = 0x1;

			public const int O_RDWR = 0x2;

			public const int O_CREAT = 0x40;

			public const int O_EXCL = 0x80;

			public const int O_NOCTTY = 0x100;

			public const int O_TRUNC = 0x200;

			public const int O_APPEND = 0x400;

			public const int O_NONBLOCK = 0x800;

			public const int O_SYNC = 0x1000;

			public const int O_ASYNC = 0x2000;

			public const int O_FSYNC = O_SYNC;

			public const int O_NDELAY = O_NONBLOCK;

			public const int POSIX_FADV_NORMAL = 0;

			public const int POSIX_FADV_RANDOM = 1;

			public const int POSIX_FADV_SEQUENTIAL = 2;

			public const int POSIX_FADV_WILLNEED = 3;

			public const int POSIX_FADV_DONTNEED = 4;

			public const int POSIX_FADV_NOREUSE = 5;

			public const int SYNC_FILE_RANGE_WAIT_BEFORE = 1;

			public const int SYNC_FILE_RANGE_WRITE = 2;

			public const int SYNC_FILE_RANGE_WAIT_AFTER = 4;

			private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
				.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.nativeio.NativeIO
				)));

			private static bool nativeLoaded = false;

			private static bool fadvisePossible = true;

			private static bool syncFileRangePossible = true;

			internal const string WORKAROUND_NON_THREADSAFE_CALLS_KEY = "hadoop.workaround.non.threadsafe.getpwuid";

			internal const bool WORKAROUND_NON_THREADSAFE_CALLS_DEFAULT = true;

			private static long cacheTimeout = -1;

			private static org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator cacheManipulator
				 = new org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator();

			// Flags for open() call from bits/fcntl.h
			// Flags for posix_fadvise() from bits/fcntl.h
			/* No further special treatment.  */
			/* Expect random page references.  */
			/* Expect sequential page references.  */
			/* Will need these pages.  */
			/* Don't need these pages.  */
			/* Data will be accessed once.  */
			/* Wait upon writeout of all pages
			in the range before performing the
			write.  */
			/* Initiate writeout of all those
			dirty pages in the range which are
			not presently under writeback.  */
			/* Wait upon writeout of all pages in
			the range after performing the
			write.  */
			public static org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator getCacheManipulator
				()
			{
				return cacheManipulator;
			}

			public static void setCacheManipulator(org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator
				 cacheManipulator)
			{
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.cacheManipulator = cacheManipulator;
			}

			/// <summary>Used to manipulate the operating system cache.</summary>
			public class CacheManipulator
			{
				/// <exception cref="System.IO.IOException"/>
				public virtual void mlock(string identifier, java.nio.ByteBuffer buffer, long len
					)
				{
					org.apache.hadoop.io.nativeio.NativeIO.POSIX.mlock(buffer, len);
				}

				public virtual long getMemlockLimit()
				{
					return org.apache.hadoop.io.nativeio.NativeIO.getMemlockLimit();
				}

				public virtual long getOperatingSystemPageSize()
				{
					return org.apache.hadoop.io.nativeio.NativeIO.getOperatingSystemPageSize();
				}

				/// <exception cref="org.apache.hadoop.io.nativeio.NativeIOException"/>
				public virtual void posixFadviseIfPossible(string identifier, java.io.FileDescriptor
					 fd, long offset, long len, int flags)
				{
					org.apache.hadoop.io.nativeio.NativeIO.POSIX.posixFadviseIfPossible(identifier, fd
						, offset, len, flags);
				}

				public virtual bool verifyCanMlock()
				{
					return org.apache.hadoop.io.nativeio.NativeIO.isAvailable();
				}
			}

			/// <summary>A CacheManipulator used for testing which does not actually call mlock.</summary>
			/// <remarks>
			/// A CacheManipulator used for testing which does not actually call mlock.
			/// This allows many tests to be run even when the operating system does not
			/// allow mlock, or only allows limited mlocking.
			/// </remarks>
			public class NoMlockCacheManipulator : org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator
			{
				/// <exception cref="System.IO.IOException"/>
				public override void mlock(string identifier, java.nio.ByteBuffer buffer, long len
					)
				{
					LOG.info("mlocking " + identifier);
				}

				public override long getMemlockLimit()
				{
					return 1125899906842624L;
				}

				public override long getOperatingSystemPageSize()
				{
					return 4096;
				}

				public override bool verifyCanMlock()
				{
					return true;
				}
			}

			static POSIX()
			{
				if (org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
				{
					try
					{
						org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
							();
						workaroundNonThreadSafePasswdCalls = conf.getBoolean(WORKAROUND_NON_THREADSAFE_CALLS_KEY
							, WORKAROUND_NON_THREADSAFE_CALLS_DEFAULT);
						initNative();
						nativeLoaded = true;
						cacheTimeout = conf.getLong(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY
							, org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT
							) * 1000;
						LOG.debug("Initialized cache for IDs to User/Group mapping with a " + " cache timeout of "
							 + cacheTimeout / 1000 + " seconds.");
					}
					catch (System.Exception t)
					{
						// This can happen if the user has an older version of libhadoop.so
						// installed - in this case we can continue without native IO
						// after warning
						org.apache.hadoop.util.PerformanceAdvisory.LOG.debug("Unable to initialize NativeIO libraries"
							, t);
					}
				}
			}

			/// <summary>Return true if the JNI-based native IO extensions are available.</summary>
			public static bool isAvailable()
			{
				return org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded() && nativeLoaded;
			}

			/// <exception cref="System.IO.IOException"/>
			private static void assertCodeLoaded()
			{
				if (!isAvailable())
				{
					throw new System.IO.IOException("NativeIO was not loaded");
				}
			}

			/// <summary>Wrapper around open(2)</summary>
			/// <exception cref="System.IO.IOException"/>
			public static java.io.FileDescriptor open(string path, int flags, int mode)
			{
			}

			/// <summary>Wrapper around fstat(2)</summary>
			/// <exception cref="System.IO.IOException"/>
			private static org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat fstat(java.io.FileDescriptor
				 fd)
			{
			}

			/// <summary>Native chmod implementation.</summary>
			/// <remarks>Native chmod implementation. On UNIX, it is a wrapper around chmod(2)</remarks>
			/// <exception cref="System.IO.IOException"/>
			private static void chmodImpl(string path, int mode)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public static void chmod(string path, int mode)
			{
				if (!org.apache.hadoop.util.Shell.WINDOWS)
				{
					chmodImpl(path, mode);
				}
				else
				{
					try
					{
						chmodImpl(path, mode);
					}
					catch (org.apache.hadoop.io.nativeio.NativeIOException nioe)
					{
						if (nioe.getErrorCode() == 3)
						{
							throw new org.apache.hadoop.io.nativeio.NativeIOException("No such file or directory"
								, org.apache.hadoop.io.nativeio.Errno.ENOENT);
						}
						else
						{
							LOG.warn(string.format("NativeIO.chmod error (%d): %s", nioe.getErrorCode(), nioe
								.Message));
							throw new org.apache.hadoop.io.nativeio.NativeIOException("Unknown error", org.apache.hadoop.io.nativeio.Errno
								.UNKNOWN);
						}
					}
				}
			}

			/// <summary>Wrapper around posix_fadvise(2)</summary>
			/// <exception cref="org.apache.hadoop.io.nativeio.NativeIOException"/>
			internal static void posix_fadvise(java.io.FileDescriptor fd, long offset, long len
				, int flags)
			{
			}

			/// <summary>Wrapper around sync_file_range(2)</summary>
			/// <exception cref="org.apache.hadoop.io.nativeio.NativeIOException"/>
			internal static void sync_file_range(java.io.FileDescriptor fd, long offset, long
				 nbytes, int flags)
			{
			}

			/// <summary>Call posix_fadvise on the given file descriptor.</summary>
			/// <remarks>
			/// Call posix_fadvise on the given file descriptor. See the manpage
			/// for this syscall for more information. On systems where this
			/// call is not available, does nothing.
			/// </remarks>
			/// <exception cref="NativeIOException">if there is an error with the syscall</exception>
			/// <exception cref="org.apache.hadoop.io.nativeio.NativeIOException"/>
			internal static void posixFadviseIfPossible(string identifier, java.io.FileDescriptor
				 fd, long offset, long len, int flags)
			{
				if (nativeLoaded && fadvisePossible)
				{
					try
					{
						posix_fadvise(fd, offset, len, flags);
					}
					catch (System.NotSupportedException)
					{
						fadvisePossible = false;
					}
					catch (java.lang.UnsatisfiedLinkError)
					{
						fadvisePossible = false;
					}
				}
			}

			/// <summary>Call sync_file_range on the given file descriptor.</summary>
			/// <remarks>
			/// Call sync_file_range on the given file descriptor. See the manpage
			/// for this syscall for more information. On systems where this
			/// call is not available, does nothing.
			/// </remarks>
			/// <exception cref="NativeIOException">if there is an error with the syscall</exception>
			/// <exception cref="org.apache.hadoop.io.nativeio.NativeIOException"/>
			public static void syncFileRangeIfPossible(java.io.FileDescriptor fd, long offset
				, long nbytes, int flags)
			{
				if (nativeLoaded && syncFileRangePossible)
				{
					try
					{
						sync_file_range(fd, offset, nbytes, flags);
					}
					catch (System.NotSupportedException)
					{
						syncFileRangePossible = false;
					}
					catch (java.lang.UnsatisfiedLinkError)
					{
						syncFileRangePossible = false;
					}
				}
			}

			/// <exception cref="org.apache.hadoop.io.nativeio.NativeIOException"/>
			internal static void mlock_native(java.nio.ByteBuffer buffer, long len)
			{
			}

			/// <summary>
			/// Locks the provided direct ByteBuffer into memory, preventing it from
			/// swapping out.
			/// </summary>
			/// <remarks>
			/// Locks the provided direct ByteBuffer into memory, preventing it from
			/// swapping out. After a buffer is locked, future accesses will not incur
			/// a page fault.
			/// See the mlock(2) man page for more information.
			/// </remarks>
			/// <exception cref="NativeIOException"/>
			/// <exception cref="System.IO.IOException"/>
			internal static void mlock(java.nio.ByteBuffer buffer, long len)
			{
				assertCodeLoaded();
				if (!buffer.isDirect())
				{
					throw new System.IO.IOException("Cannot mlock a non-direct ByteBuffer");
				}
				mlock_native(buffer, len);
			}

			/// <summary>Unmaps the block from memory.</summary>
			/// <remarks>
			/// Unmaps the block from memory. See munmap(2).
			/// There isn't any portable way to unmap a memory region in Java.
			/// So we use the sun.nio method here.
			/// Note that unmapping a memory region could cause crashes if code
			/// continues to reference the unmapped code.  However, if we don't
			/// manually unmap the memory, we are dependent on the finalizer to
			/// do it, and we have no idea when the finalizer will run.
			/// </remarks>
			/// <param name="buffer">The buffer to unmap.</param>
			public static void munmap(java.nio.MappedByteBuffer buffer)
			{
				if (buffer is sun.nio.ch.DirectBuffer)
				{
					sun.misc.Cleaner cleaner = ((sun.nio.ch.DirectBuffer)buffer).cleaner();
					cleaner.clean();
				}
			}

			/// <summary>Linux only methods used for getOwner() implementation</summary>
			/// <exception cref="System.IO.IOException"/>
			private static long getUIDforFDOwnerforOwner(java.io.FileDescriptor fd)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			private static string getUserName(long uid)
			{
			}

			/// <summary>Result type of the fstat call</summary>
			public class Stat
			{
				private int ownerId;

				private int groupId;

				private string owner;

				private string group;

				private int mode;

				public const int S_IFMT = 0xf000;

				public const int S_IFIFO = 0x1000;

				public const int S_IFCHR = 0x2000;

				public const int S_IFDIR = 0x4000;

				public const int S_IFBLK = 0x6000;

				public const int S_IFREG = 0x8000;

				public const int S_IFLNK = 0xa000;

				public const int S_IFSOCK = 0xc000;

				public const int S_IFWHT = 0xe000;

				public const int S_ISUID = 0x800;

				public const int S_ISGID = 0x400;

				public const int S_ISVTX = 0x200;

				public const int S_IRUSR = 0x100;

				public const int S_IWUSR = 0x80;

				public const int S_IXUSR = 0x40;

				internal Stat(int ownerId, int groupId, int mode)
				{
					// Mode constants
					/* type of file */
					/* named pipe (fifo) */
					/* character special */
					/* directory */
					/* block special */
					/* regular */
					/* symbolic link */
					/* socket */
					/* whiteout */
					/* set user id on execution */
					/* set group id on execution */
					/* save swapped text even after use */
					/* read permission, owner */
					/* write permission, owner */
					/* execute/search permission, owner */
					this.ownerId = ownerId;
					this.groupId = groupId;
					this.mode = mode;
				}

				internal Stat(string owner, string group, int mode)
				{
					if (!org.apache.hadoop.util.Shell.WINDOWS)
					{
						this.owner = owner;
					}
					else
					{
						this.owner = stripDomain(owner);
					}
					if (!org.apache.hadoop.util.Shell.WINDOWS)
					{
						this.group = group;
					}
					else
					{
						this.group = stripDomain(group);
					}
					this.mode = mode;
				}

				public override string ToString()
				{
					return "Stat(owner='" + owner + "', group='" + group + "'" + ", mode=" + mode + ")";
				}

				public virtual string getOwner()
				{
					return owner;
				}

				public virtual string getGroup()
				{
					return group;
				}

				public virtual int getMode()
				{
					return mode;
				}
			}

			/// <summary>Returns the file stat for a file descriptor.</summary>
			/// <param name="fd">file descriptor.</param>
			/// <returns>the file descriptor file stat.</returns>
			/// <exception cref="System.IO.IOException">thrown if there was an IO error while obtaining the file stat.
			/// 	</exception>
			public static org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat getFstat(java.io.FileDescriptor
				 fd)
			{
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat stat = null;
				if (!org.apache.hadoop.util.Shell.WINDOWS)
				{
					stat = fstat(fd);
					stat.owner = getName(org.apache.hadoop.io.nativeio.NativeIO.POSIX.IdCache.USER, stat
						.ownerId);
					stat.group = getName(org.apache.hadoop.io.nativeio.NativeIO.POSIX.IdCache.GROUP, 
						stat.groupId);
				}
				else
				{
					try
					{
						stat = fstat(fd);
					}
					catch (org.apache.hadoop.io.nativeio.NativeIOException nioe)
					{
						if (nioe.getErrorCode() == 6)
						{
							throw new org.apache.hadoop.io.nativeio.NativeIOException("The handle is invalid."
								, org.apache.hadoop.io.nativeio.Errno.EBADF);
						}
						else
						{
							LOG.warn(string.format("NativeIO.getFstat error (%d): %s", nioe.getErrorCode(), nioe
								.Message));
							throw new org.apache.hadoop.io.nativeio.NativeIOException("Unknown error", org.apache.hadoop.io.nativeio.Errno
								.UNKNOWN);
						}
					}
				}
				return stat;
			}

			/// <exception cref="System.IO.IOException"/>
			private static string getName(org.apache.hadoop.io.nativeio.NativeIO.POSIX.IdCache
				 domain, int id)
			{
				System.Collections.Generic.IDictionary<int, org.apache.hadoop.io.nativeio.NativeIO.POSIX.CachedName
					> idNameCache = (domain == org.apache.hadoop.io.nativeio.NativeIO.POSIX.IdCache.
					USER) ? USER_ID_NAME_CACHE : GROUP_ID_NAME_CACHE;
				string name;
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.CachedName cachedName = idNameCache[
					id];
				long now = Sharpen.Runtime.currentTimeMillis();
				if (cachedName != null && (cachedName.timestamp + cacheTimeout) > now)
				{
					name = cachedName.name;
				}
				else
				{
					name = (domain == org.apache.hadoop.io.nativeio.NativeIO.POSIX.IdCache.USER) ? getUserName
						(id) : getGroupName(id);
					if (LOG.isDebugEnabled())
					{
						string type = (domain == org.apache.hadoop.io.nativeio.NativeIO.POSIX.IdCache.USER
							) ? "UserName" : "GroupName";
						LOG.debug("Got " + type + " " + name + " for ID " + id + " from the native implementation"
							);
					}
					cachedName = new org.apache.hadoop.io.nativeio.NativeIO.POSIX.CachedName(name, now
						);
					idNameCache[id] = cachedName;
				}
				return name;
			}

			/// <exception cref="System.IO.IOException"/>
			internal static string getUserName(int uid)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal static string getGroupName(int uid)
			{
			}

			private class CachedName
			{
				internal readonly long timestamp;

				internal readonly string name;

				public CachedName(string name, long timestamp)
				{
					this.name = name;
					this.timestamp = timestamp;
				}
			}

			private static readonly System.Collections.Generic.IDictionary<int, org.apache.hadoop.io.nativeio.NativeIO.POSIX.CachedName
				> USER_ID_NAME_CACHE = new java.util.concurrent.ConcurrentHashMap<int, org.apache.hadoop.io.nativeio.NativeIO.POSIX.CachedName
				>();

			private static readonly System.Collections.Generic.IDictionary<int, org.apache.hadoop.io.nativeio.NativeIO.POSIX.CachedName
				> GROUP_ID_NAME_CACHE = new java.util.concurrent.ConcurrentHashMap<int, org.apache.hadoop.io.nativeio.NativeIO.POSIX.CachedName
				>();

			private enum IdCache
			{
				USER,
				GROUP
			}

			public const int MMAP_PROT_READ = unchecked((int)(0x1));

			public const int MMAP_PROT_WRITE = unchecked((int)(0x2));

			public const int MMAP_PROT_EXEC = unchecked((int)(0x4));

			/// <exception cref="System.IO.IOException"/>
			public static long mmap(java.io.FileDescriptor fd, int prot, bool shared, long length
				)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public static void munmap(long addr, long length)
			{
			}
		}

		private static bool workaroundNonThreadSafePasswdCalls = false;

		public class Windows
		{
			public const long GENERIC_READ = unchecked((long)(0x80000000L));

			public const long GENERIC_WRITE = unchecked((long)(0x40000000L));

			public const long FILE_SHARE_READ = unchecked((long)(0x00000001L));

			public const long FILE_SHARE_WRITE = unchecked((long)(0x00000002L));

			public const long FILE_SHARE_DELETE = unchecked((long)(0x00000004L));

			public const long CREATE_NEW = 1;

			public const long CREATE_ALWAYS = 2;

			public const long OPEN_EXISTING = 3;

			public const long OPEN_ALWAYS = 4;

			public const long TRUNCATE_EXISTING = 5;

			public const long FILE_BEGIN = 0;

			public const long FILE_CURRENT = 1;

			public const long FILE_END = 2;

			public const long FILE_ATTRIBUTE_NORMAL = unchecked((long)(0x00000080L));

			// Flags for CreateFile() call on Windows
			/// <summary>Create a directory with permissions set to the specified mode.</summary>
			/// <remarks>
			/// Create a directory with permissions set to the specified mode.  By setting
			/// permissions at creation time, we avoid issues related to the user lacking
			/// WRITE_DAC rights on subsequent chmod calls.  One example where this can
			/// occur is writing to an SMB share where the user does not have Full Control
			/// rights, and therefore WRITE_DAC is denied.
			/// </remarks>
			/// <param name="path">directory to create</param>
			/// <param name="mode">permissions of new directory</param>
			/// <exception cref="System.IO.IOException">if there is an I/O error</exception>
			public static void createDirectoryWithMode(java.io.File path, int mode)
			{
				createDirectoryWithMode0(path.getAbsolutePath(), mode);
			}

			/// <summary>Wrapper around CreateDirectory() on Windows</summary>
			/// <exception cref="org.apache.hadoop.io.nativeio.NativeIOException"/>
			private static void createDirectoryWithMode0(string path, int mode)
			{
			}

			/// <summary>Wrapper around CreateFile() on Windows</summary>
			/// <exception cref="System.IO.IOException"/>
			public static java.io.FileDescriptor createFile(string path, long desiredAccess, 
				long shareMode, long creationDisposition)
			{
			}

			/// <summary>Create a file for write with permissions set to the specified mode.</summary>
			/// <remarks>
			/// Create a file for write with permissions set to the specified mode.  By
			/// setting permissions at creation time, we avoid issues related to the user
			/// lacking WRITE_DAC rights on subsequent chmod calls.  One example where
			/// this can occur is writing to an SMB share where the user does not have
			/// Full Control rights, and therefore WRITE_DAC is denied.
			/// This method mimics the semantics implemented by the JDK in
			/// <see cref="java.io.FileOutputStream"/>
			/// .  The file is opened for truncate or
			/// append, the sharing mode allows other readers and writers, and paths
			/// longer than MAX_PATH are supported.  (See io_util_md.c in the JDK.)
			/// </remarks>
			/// <param name="path">file to create</param>
			/// <param name="append">if true, then open file for append</param>
			/// <param name="mode">permissions of new directory</param>
			/// <returns>FileOutputStream of opened file</returns>
			/// <exception cref="System.IO.IOException">if there is an I/O error</exception>
			public static java.io.FileOutputStream createFileOutputStreamWithMode(java.io.File
				 path, bool append, int mode)
			{
				long desiredAccess = GENERIC_WRITE;
				long shareMode = FILE_SHARE_READ | FILE_SHARE_WRITE;
				long creationDisposition = append ? OPEN_ALWAYS : CREATE_ALWAYS;
				return new java.io.FileOutputStream(createFileWithMode0(path.getAbsolutePath(), desiredAccess
					, shareMode, creationDisposition, mode));
			}

			/// <summary>Wrapper around CreateFile() with security descriptor on Windows</summary>
			/// <exception cref="org.apache.hadoop.io.nativeio.NativeIOException"/>
			private static java.io.FileDescriptor createFileWithMode0(string path, long desiredAccess
				, long shareMode, long creationDisposition, int mode)
			{
			}

			/// <summary>Wrapper around SetFilePointer() on Windows</summary>
			/// <exception cref="System.IO.IOException"/>
			public static long setFilePointer(java.io.FileDescriptor fd, long distanceToMove, 
				long moveMethod)
			{
			}

			/// <summary>Windows only methods used for getOwner() implementation</summary>
			/// <exception cref="System.IO.IOException"/>
			private static string getOwner(java.io.FileDescriptor fd)
			{
			}

			/// <summary>Supported list of Windows access right flags</summary>
			[System.Serializable]
			public sealed class AccessRight
			{
				public static readonly org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
					 ACCESS_READ = new org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight(unchecked(
					(int)(0x0001)));

				public static readonly org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
					 ACCESS_WRITE = new org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight(unchecked(
					(int)(0x0002)));

				public static readonly org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
					 ACCESS_EXECUTE = new org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
					(unchecked((int)(0x0020)));

				private readonly int accessRight;

				internal AccessRight(int access)
				{
					// FILE_READ_DATA
					// FILE_WRITE_DATA
					// FILE_EXECUTE
					org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight.accessRight = access;
				}

				public int accessRight()
				{
					return org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight.accessRight;
				}
			}

			/// <summary>
			/// Windows only method used to check if the current process has requested
			/// access rights on the given path.
			/// </summary>
			private static bool access0(string path, int requestedAccess)
			{
			}

			/// <summary>
			/// Checks whether the current process has desired access rights on
			/// the given path.
			/// </summary>
			/// <remarks>
			/// Checks whether the current process has desired access rights on
			/// the given path.
			/// Longer term this native function can be substituted with JDK7
			/// function Files#isReadable, isWritable, isExecutable.
			/// </remarks>
			/// <param name="path">input path</param>
			/// <param name="desiredAccess">ACCESS_READ, ACCESS_WRITE or ACCESS_EXECUTE</param>
			/// <returns>true if access is allowed</returns>
			/// <exception cref="System.IO.IOException">I/O exception on error</exception>
			public static bool access(string path, org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
				 desiredAccess)
			{
				return access0(path, desiredAccess.accessRight());
			}

			/// <summary>
			/// Extends both the minimum and maximum working set size of the current
			/// process.
			/// </summary>
			/// <remarks>
			/// Extends both the minimum and maximum working set size of the current
			/// process.  This method gets the current minimum and maximum working set
			/// size, adds the requested amount to each and then sets the minimum and
			/// maximum working set size to the new values.  Controlling the working set
			/// size of the process also controls the amount of memory it can lock.
			/// </remarks>
			/// <param name="delta">amount to increment minimum and maximum working set size</param>
			/// <exception cref="System.IO.IOException">for any error</exception>
			/// <seealso cref="POSIX.mlock(java.nio.ByteBuffer, long)"/>
			public static void extendWorkingSetSize(long delta)
			{
			}

			static Windows()
			{
				if (org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
				{
					try
					{
						initNative();
						nativeLoaded = true;
					}
					catch (System.Exception t)
					{
						// This can happen if the user has an older version of libhadoop.so
						// installed - in this case we can continue without native IO
						// after warning
						org.apache.hadoop.util.PerformanceAdvisory.LOG.debug("Unable to initialize NativeIO libraries"
							, t);
					}
				}
			}
		}

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.nativeio.NativeIO
			)));

		private static bool nativeLoaded = false;

		static NativeIO()
		{
			if (org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
			{
				try
				{
					initNative();
					nativeLoaded = true;
				}
				catch (System.Exception t)
				{
					// This can happen if the user has an older version of libhadoop.so
					// installed - in this case we can continue without native IO
					// after warning
					org.apache.hadoop.util.PerformanceAdvisory.LOG.debug("Unable to initialize NativeIO libraries"
						, t);
				}
			}
		}

		/// <summary>Return true if the JNI-based native IO extensions are available.</summary>
		public static bool isAvailable()
		{
			return org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded() && nativeLoaded;
		}

		/// <summary>Initialize the JNI method ID and class ID cache</summary>
		private static void initNative()
		{
		}

		/// <summary>
		/// Get the maximum number of bytes that can be locked into memory at any
		/// given point.
		/// </summary>
		/// <returns>
		/// 0 if no bytes can be locked into memory;
		/// Long.MAX_VALUE if there is no limit;
		/// The number of bytes that can be locked into memory otherwise.
		/// </returns>
		internal static long getMemlockLimit()
		{
			return isAvailable() ? getMemlockLimit0() : 0;
		}

		private static long getMemlockLimit0()
		{
		}

		/// <returns>the operating system's page size.</returns>
		internal static long getOperatingSystemPageSize()
		{
			try
			{
				java.lang.reflect.Field f = Sharpen.Runtime.getClassForType(typeof(sun.misc.Unsafe
					)).getDeclaredField("theUnsafe");
				f.setAccessible(true);
				sun.misc.Unsafe @unsafe = (sun.misc.Unsafe)f.get(null);
				return @unsafe.pageSize();
			}
			catch (System.Exception e)
			{
				LOG.warn("Unable to get operating system page size.  Guessing 4096.", e);
				return 4096;
			}
		}

		private class CachedUid
		{
			internal readonly long timestamp;

			internal readonly string username;

			public CachedUid(string username, long timestamp)
			{
				this.timestamp = timestamp;
				this.username = username;
			}
		}

		private static readonly System.Collections.Generic.IDictionary<long, org.apache.hadoop.io.nativeio.NativeIO.CachedUid
			> uidCache = new java.util.concurrent.ConcurrentHashMap<long, org.apache.hadoop.io.nativeio.NativeIO.CachedUid
			>();

		private static long cacheTimeout;

		private static bool initialized = false;

		/// <summary>
		/// The Windows logon name has two part, NetBIOS domain name and
		/// user account name, of the format DOMAIN\UserName.
		/// </summary>
		/// <remarks>
		/// The Windows logon name has two part, NetBIOS domain name and
		/// user account name, of the format DOMAIN\UserName. This method
		/// will remove the domain part of the full logon name.
		/// </remarks>
		/// <param name="Fthe">full principal name containing the domain</param>
		/// <returns>name with domain removed</returns>
		private static string stripDomain(string name)
		{
			int i = name.IndexOf('\\');
			if (i != -1)
			{
				name = Sharpen.Runtime.substring(name, i + 1);
			}
			return name;
		}

		/// <exception cref="System.IO.IOException"/>
		public static string getOwner(java.io.FileDescriptor fd)
		{
			ensureInitialized();
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				string owner = org.apache.hadoop.io.nativeio.NativeIO.Windows.getOwner(fd);
				owner = stripDomain(owner);
				return owner;
			}
			else
			{
				long uid = org.apache.hadoop.io.nativeio.NativeIO.POSIX.getUIDforFDOwnerforOwner(
					fd);
				org.apache.hadoop.io.nativeio.NativeIO.CachedUid cUid = uidCache[uid];
				long now = Sharpen.Runtime.currentTimeMillis();
				if (cUid != null && (cUid.timestamp + cacheTimeout) > now)
				{
					return cUid.username;
				}
				string user = org.apache.hadoop.io.nativeio.NativeIO.POSIX.getUserName(uid);
				LOG.info("Got UserName " + user + " for UID " + uid + " from the native implementation"
					);
				cUid = new org.apache.hadoop.io.nativeio.NativeIO.CachedUid(user, now);
				uidCache[uid] = cUid;
				return user;
			}
		}

		/// <summary>
		/// Create a FileInputStream that shares delete permission on the
		/// file opened, i.e.
		/// </summary>
		/// <remarks>
		/// Create a FileInputStream that shares delete permission on the
		/// file opened, i.e. other process can delete the file the
		/// FileInputStream is reading. Only Windows implementation uses
		/// the native interface.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static java.io.FileInputStream getShareDeleteFileInputStream(java.io.File 
			f)
		{
			if (!org.apache.hadoop.util.Shell.WINDOWS)
			{
				// On Linux the default FileInputStream shares delete permission
				// on the file opened.
				//
				return new java.io.FileInputStream(f);
			}
			else
			{
				// Use Windows native interface to create a FileInputStream that
				// shares delete permission on the file opened.
				//
				java.io.FileDescriptor fd = org.apache.hadoop.io.nativeio.NativeIO.Windows.createFile
					(f.getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.GENERIC_READ
					, org.apache.hadoop.io.nativeio.NativeIO.Windows.FILE_SHARE_READ | org.apache.hadoop.io.nativeio.NativeIO.Windows
					.FILE_SHARE_WRITE | org.apache.hadoop.io.nativeio.NativeIO.Windows.FILE_SHARE_DELETE
					, org.apache.hadoop.io.nativeio.NativeIO.Windows.OPEN_EXISTING);
				return new java.io.FileInputStream(fd);
			}
		}

		/// <summary>
		/// Create a FileInputStream that shares delete permission on the
		/// file opened at a given offset, i.e.
		/// </summary>
		/// <remarks>
		/// Create a FileInputStream that shares delete permission on the
		/// file opened at a given offset, i.e. other process can delete
		/// the file the FileInputStream is reading. Only Windows implementation
		/// uses the native interface.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static java.io.FileInputStream getShareDeleteFileInputStream(java.io.File 
			f, long seekOffset)
		{
			if (!org.apache.hadoop.util.Shell.WINDOWS)
			{
				java.io.RandomAccessFile rf = new java.io.RandomAccessFile(f, "r");
				if (seekOffset > 0)
				{
					rf.seek(seekOffset);
				}
				return new java.io.FileInputStream(rf.getFD());
			}
			else
			{
				// Use Windows native interface to create a FileInputStream that
				// shares delete permission on the file opened, and set it to the
				// given offset.
				//
				java.io.FileDescriptor fd = org.apache.hadoop.io.nativeio.NativeIO.Windows.createFile
					(f.getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.GENERIC_READ
					, org.apache.hadoop.io.nativeio.NativeIO.Windows.FILE_SHARE_READ | org.apache.hadoop.io.nativeio.NativeIO.Windows
					.FILE_SHARE_WRITE | org.apache.hadoop.io.nativeio.NativeIO.Windows.FILE_SHARE_DELETE
					, org.apache.hadoop.io.nativeio.NativeIO.Windows.OPEN_EXISTING);
				if (seekOffset > 0)
				{
					org.apache.hadoop.io.nativeio.NativeIO.Windows.setFilePointer(fd, seekOffset, org.apache.hadoop.io.nativeio.NativeIO.Windows
						.FILE_BEGIN);
				}
				return new java.io.FileInputStream(fd);
			}
		}

		/// <summary>Create the specified File for write access, ensuring that it does not exist.
		/// 	</summary>
		/// <param name="f">the file that we want to create</param>
		/// <param name="permissions">we want to have on the file (if security is enabled)</param>
		/// <exception cref="org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException">if the file already exists
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if any other error occurred</exception>
		public static java.io.FileOutputStream getCreateForWriteFileOutputStream(java.io.File
			 f, int permissions)
		{
			if (!org.apache.hadoop.util.Shell.WINDOWS)
			{
				// Use the native wrapper around open(2)
				try
				{
					java.io.FileDescriptor fd = org.apache.hadoop.io.nativeio.NativeIO.POSIX.open(f.getAbsolutePath
						(), org.apache.hadoop.io.nativeio.NativeIO.POSIX.O_WRONLY | org.apache.hadoop.io.nativeio.NativeIO.POSIX
						.O_CREAT | org.apache.hadoop.io.nativeio.NativeIO.POSIX.O_EXCL, permissions);
					return new java.io.FileOutputStream(fd);
				}
				catch (org.apache.hadoop.io.nativeio.NativeIOException nioe)
				{
					if (nioe.getErrno() == org.apache.hadoop.io.nativeio.Errno.EEXIST)
					{
						throw new org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException(nioe);
					}
					throw;
				}
			}
			else
			{
				// Use the Windows native APIs to create equivalent FileOutputStream
				try
				{
					java.io.FileDescriptor fd = org.apache.hadoop.io.nativeio.NativeIO.Windows.createFile
						(f.getCanonicalPath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.GENERIC_WRITE
						, org.apache.hadoop.io.nativeio.NativeIO.Windows.FILE_SHARE_DELETE | org.apache.hadoop.io.nativeio.NativeIO.Windows
						.FILE_SHARE_READ | org.apache.hadoop.io.nativeio.NativeIO.Windows.FILE_SHARE_WRITE
						, org.apache.hadoop.io.nativeio.NativeIO.Windows.CREATE_NEW);
					org.apache.hadoop.io.nativeio.NativeIO.POSIX.chmod(f.getCanonicalPath(), permissions
						);
					return new java.io.FileOutputStream(fd);
				}
				catch (org.apache.hadoop.io.nativeio.NativeIOException nioe)
				{
					if (nioe.getErrorCode() == 80)
					{
						// ERROR_FILE_EXISTS
						// 80 (0x50)
						// The file exists
						throw new org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException(nioe);
					}
					throw;
				}
			}
		}

		private static void ensureInitialized()
		{
			lock (typeof(NativeIO))
			{
				if (!initialized)
				{
					cacheTimeout = new org.apache.hadoop.conf.Configuration().getLong("hadoop.security.uid.cache.secs"
						, 4 * 60 * 60) * 1000;
					LOG.info("Initialized cache for UID to User mapping with a cache" + " timeout of "
						 + cacheTimeout / 1000 + " seconds.");
					initialized = true;
				}
			}
		}

		/// <summary>A version of renameTo that throws a descriptive exception when it fails.
		/// 	</summary>
		/// <param name="src">The source path</param>
		/// <param name="dst">The destination path</param>
		/// <exception cref="NativeIOException">On failure.</exception>
		/// <exception cref="System.IO.IOException"/>
		public static void renameTo(java.io.File src, java.io.File dst)
		{
			if (!nativeLoaded)
			{
				if (!src.renameTo(dst))
				{
					throw new System.IO.IOException("renameTo(src=" + src + ", dst=" + dst + ") failed."
						);
				}
			}
			else
			{
				renameTo0(src.getAbsolutePath(), dst.getAbsolutePath());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void link(java.io.File src, java.io.File dst)
		{
			if (!nativeLoaded)
			{
				org.apache.hadoop.fs.HardLink.createHardLink(src, dst);
			}
			else
			{
				link0(src.getAbsolutePath(), dst.getAbsolutePath());
			}
		}

		/// <summary>A version of renameTo that throws a descriptive exception when it fails.
		/// 	</summary>
		/// <param name="src">The source path</param>
		/// <param name="dst">The destination path</param>
		/// <exception cref="NativeIOException">On failure.</exception>
		/// <exception cref="org.apache.hadoop.io.nativeio.NativeIOException"/>
		private static void renameTo0(string src, string dst)
		{
		}

		/// <exception cref="org.apache.hadoop.io.nativeio.NativeIOException"/>
		private static void link0(string src, string dst)
		{
		}

		/// <summary>
		/// Unbuffered file copy from src to dst without tainting OS buffer cache
		/// In POSIX platform:
		/// It uses FileChannel#transferTo() which internally attempts
		/// unbuffered IO on OS with native sendfile64() support and falls back to
		/// buffered IO otherwise.
		/// </summary>
		/// <remarks>
		/// Unbuffered file copy from src to dst without tainting OS buffer cache
		/// In POSIX platform:
		/// It uses FileChannel#transferTo() which internally attempts
		/// unbuffered IO on OS with native sendfile64() support and falls back to
		/// buffered IO otherwise.
		/// It minimizes the number of FileChannel#transferTo call by passing the the
		/// src file size directly instead of a smaller size as the 3rd parameter.
		/// This saves the number of sendfile64() system call when native sendfile64()
		/// is supported. In the two fall back cases where sendfile is not supported,
		/// FileChannle#transferTo already has its own batching of size 8 MB and 8 KB,
		/// respectively.
		/// In Windows Platform:
		/// It uses its own native wrapper of CopyFileEx with COPY_FILE_NO_BUFFERING
		/// flag, which is supported on Windows Server 2008 and above.
		/// Ideally, we should use FileChannel#transferTo() across both POSIX and Windows
		/// platform. Unfortunately, the wrapper(Java_sun_nio_ch_FileChannelImpl_transferTo0)
		/// used by FileChannel#transferTo for unbuffered IO is not implemented on Windows.
		/// Based on OpenJDK 6/7/8 source code, Java_sun_nio_ch_FileChannelImpl_transferTo0
		/// on Windows simply returns IOS_UNSUPPORTED.
		/// Note: This simple native wrapper does minimal parameter checking before copy and
		/// consistency check (e.g., size) after copy.
		/// It is recommended to use wrapper function like
		/// the Storage#nativeCopyFileUnbuffered() function in hadoop-hdfs with pre/post copy
		/// checks.
		/// </remarks>
		/// <param name="src">The source path</param>
		/// <param name="dst">The destination path</param>
		/// <exception cref="System.IO.IOException"/>
		public static void copyFileUnbuffered(java.io.File src, java.io.File dst)
		{
			if (nativeLoaded && org.apache.hadoop.util.Shell.WINDOWS)
			{
				copyFileUnbuffered0(src.getAbsolutePath(), dst.getAbsolutePath());
			}
			else
			{
				java.io.FileInputStream fis = null;
				java.io.FileOutputStream fos = null;
				java.nio.channels.FileChannel input = null;
				java.nio.channels.FileChannel output = null;
				try
				{
					fis = new java.io.FileInputStream(src);
					fos = new java.io.FileOutputStream(dst);
					input = fis.getChannel();
					output = fos.getChannel();
					long remaining = input.size();
					long position = 0;
					long transferred = 0;
					while (remaining > 0)
					{
						transferred = input.transferTo(position, remaining, output);
						remaining -= transferred;
						position += transferred;
					}
				}
				finally
				{
					org.apache.hadoop.io.IOUtils.cleanup(LOG, output);
					org.apache.hadoop.io.IOUtils.cleanup(LOG, fos);
					org.apache.hadoop.io.IOUtils.cleanup(LOG, input);
					org.apache.hadoop.io.IOUtils.cleanup(LOG, fis);
				}
			}
		}

		/// <exception cref="org.apache.hadoop.io.nativeio.NativeIOException"/>
		private static void copyFileUnbuffered0(string src, string dst)
		{
		}
	}
}
