using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sun.Misc;
using Sun.Nio.CH;

namespace Org.Apache.Hadoop.IO.Nativeio
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
			public const int ORdonly = 00;

			public const int OWronly = 0x1;

			public const int ORdwr = 0x2;

			public const int OCreat = 0x40;

			public const int OExcl = 0x80;

			public const int ONoctty = 0x100;

			public const int OTrunc = 0x200;

			public const int OAppend = 0x400;

			public const int ONonblock = 0x800;

			public const int OSync = 0x1000;

			public const int OAsync = 0x2000;

			public const int OFsync = OSync;

			public const int ONdelay = ONonblock;

			public const int PosixFadvNormal = 0;

			public const int PosixFadvRandom = 1;

			public const int PosixFadvSequential = 2;

			public const int PosixFadvWillneed = 3;

			public const int PosixFadvDontneed = 4;

			public const int PosixFadvNoreuse = 5;

			public const int SyncFileRangeWaitBefore = 1;

			public const int SyncFileRangeWrite = 2;

			public const int SyncFileRangeWaitAfter = 4;

			private static readonly Log Log = LogFactory.GetLog(typeof(NativeIO));

			private static bool nativeLoaded = false;

			private static bool fadvisePossible = true;

			private static bool syncFileRangePossible = true;

			internal const string WorkaroundNonThreadsafeCallsKey = "hadoop.workaround.non.threadsafe.getpwuid";

			internal const bool WorkaroundNonThreadsafeCallsDefault = true;

			private static long cacheTimeout = -1;

			private static NativeIO.POSIX.CacheManipulator cacheManipulator = new NativeIO.POSIX.CacheManipulator
				();

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
			public static NativeIO.POSIX.CacheManipulator GetCacheManipulator()
			{
				return cacheManipulator;
			}

			public static void SetCacheManipulator(NativeIO.POSIX.CacheManipulator cacheManipulator
				)
			{
				NativeIO.POSIX.cacheManipulator = cacheManipulator;
			}

			/// <summary>Used to manipulate the operating system cache.</summary>
			public class CacheManipulator
			{
				/// <exception cref="System.IO.IOException"/>
				public virtual void Mlock(string identifier, ByteBuffer buffer, long len)
				{
					NativeIO.POSIX.Mlock(buffer, len);
				}

				public virtual long GetMemlockLimit()
				{
					return NativeIO.GetMemlockLimit();
				}

				public virtual long GetOperatingSystemPageSize()
				{
					return NativeIO.GetOperatingSystemPageSize();
				}

				/// <exception cref="Org.Apache.Hadoop.IO.Nativeio.NativeIOException"/>
				public virtual void PosixFadviseIfPossible(string identifier, FileDescriptor fd, 
					long offset, long len, int flags)
				{
					NativeIO.POSIX.PosixFadviseIfPossible(identifier, fd, offset, len, flags);
				}

				public virtual bool VerifyCanMlock()
				{
					return NativeIO.IsAvailable();
				}
			}

			/// <summary>A CacheManipulator used for testing which does not actually call mlock.</summary>
			/// <remarks>
			/// A CacheManipulator used for testing which does not actually call mlock.
			/// This allows many tests to be run even when the operating system does not
			/// allow mlock, or only allows limited mlocking.
			/// </remarks>
			public class NoMlockCacheManipulator : NativeIO.POSIX.CacheManipulator
			{
				/// <exception cref="System.IO.IOException"/>
				public override void Mlock(string identifier, ByteBuffer buffer, long len)
				{
					Log.Info("mlocking " + identifier);
				}

				public override long GetMemlockLimit()
				{
					return 1125899906842624L;
				}

				public override long GetOperatingSystemPageSize()
				{
					return 4096;
				}

				public override bool VerifyCanMlock()
				{
					return true;
				}
			}

			static POSIX()
			{
				if (NativeCodeLoader.IsNativeCodeLoaded())
				{
					try
					{
						Configuration conf = new Configuration();
						workaroundNonThreadSafePasswdCalls = conf.GetBoolean(WorkaroundNonThreadsafeCallsKey
							, WorkaroundNonThreadsafeCallsDefault);
						InitNative();
						nativeLoaded = true;
						cacheTimeout = conf.GetLong(CommonConfigurationKeys.HadoopSecurityUidNameCacheTimeoutKey
							, CommonConfigurationKeys.HadoopSecurityUidNameCacheTimeoutDefault) * 1000;
						Log.Debug("Initialized cache for IDs to User/Group mapping with a " + " cache timeout of "
							 + cacheTimeout / 1000 + " seconds.");
					}
					catch (Exception t)
					{
						// This can happen if the user has an older version of libhadoop.so
						// installed - in this case we can continue without native IO
						// after warning
						PerformanceAdvisory.Log.Debug("Unable to initialize NativeIO libraries", t);
					}
				}
			}

			/// <summary>Return true if the JNI-based native IO extensions are available.</summary>
			public static bool IsAvailable()
			{
				return NativeCodeLoader.IsNativeCodeLoaded() && nativeLoaded;
			}

			/// <exception cref="System.IO.IOException"/>
			private static void AssertCodeLoaded()
			{
				if (!IsAvailable())
				{
					throw new IOException("NativeIO was not loaded");
				}
			}

			/// <summary>Wrapper around open(2)</summary>
			/// <exception cref="System.IO.IOException"/>
			public static FileDescriptor Open(string path, int flags, int mode)
			{
			}

			/// <summary>Wrapper around fstat(2)</summary>
			/// <exception cref="System.IO.IOException"/>
			private static NativeIO.POSIX.Stat Fstat(FileDescriptor fd)
			{
			}

			/// <summary>Native chmod implementation.</summary>
			/// <remarks>Native chmod implementation. On UNIX, it is a wrapper around chmod(2)</remarks>
			/// <exception cref="System.IO.IOException"/>
			private static void ChmodImpl(string path, int mode)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public static void Chmod(string path, int mode)
			{
				if (!Shell.Windows)
				{
					ChmodImpl(path, mode);
				}
				else
				{
					try
					{
						ChmodImpl(path, mode);
					}
					catch (NativeIOException nioe)
					{
						if (nioe.GetErrorCode() == 3)
						{
							throw new NativeIOException("No such file or directory", Errno.Enoent);
						}
						else
						{
							Log.Warn(string.Format("NativeIO.chmod error (%d): %s", nioe.GetErrorCode(), nioe
								.Message));
							throw new NativeIOException("Unknown error", Errno.Unknown);
						}
					}
				}
			}

			/// <summary>Wrapper around posix_fadvise(2)</summary>
			/// <exception cref="Org.Apache.Hadoop.IO.Nativeio.NativeIOException"/>
			internal static void Posix_fadvise(FileDescriptor fd, long offset, long len, int 
				flags)
			{
			}

			/// <summary>Wrapper around sync_file_range(2)</summary>
			/// <exception cref="Org.Apache.Hadoop.IO.Nativeio.NativeIOException"/>
			internal static void Sync_file_range(FileDescriptor fd, long offset, long nbytes, 
				int flags)
			{
			}

			/// <summary>Call posix_fadvise on the given file descriptor.</summary>
			/// <remarks>
			/// Call posix_fadvise on the given file descriptor. See the manpage
			/// for this syscall for more information. On systems where this
			/// call is not available, does nothing.
			/// </remarks>
			/// <exception cref="NativeIOException">if there is an error with the syscall</exception>
			/// <exception cref="Org.Apache.Hadoop.IO.Nativeio.NativeIOException"/>
			internal static void PosixFadviseIfPossible(string identifier, FileDescriptor fd, 
				long offset, long len, int flags)
			{
				if (nativeLoaded && fadvisePossible)
				{
					try
					{
						Posix_fadvise(fd, offset, len, flags);
					}
					catch (NotSupportedException)
					{
						fadvisePossible = false;
					}
					catch (UnsatisfiedLinkError)
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
			/// <exception cref="Org.Apache.Hadoop.IO.Nativeio.NativeIOException"/>
			public static void SyncFileRangeIfPossible(FileDescriptor fd, long offset, long nbytes
				, int flags)
			{
				if (nativeLoaded && syncFileRangePossible)
				{
					try
					{
						Sync_file_range(fd, offset, nbytes, flags);
					}
					catch (NotSupportedException)
					{
						syncFileRangePossible = false;
					}
					catch (UnsatisfiedLinkError)
					{
						syncFileRangePossible = false;
					}
				}
			}

			/// <exception cref="Org.Apache.Hadoop.IO.Nativeio.NativeIOException"/>
			internal static void Mlock_native(ByteBuffer buffer, long len)
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
			internal static void Mlock(ByteBuffer buffer, long len)
			{
				AssertCodeLoaded();
				if (!buffer.IsDirect())
				{
					throw new IOException("Cannot mlock a non-direct ByteBuffer");
				}
				Mlock_native(buffer, len);
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
			public static void Munmap(MappedByteBuffer buffer)
			{
				if (buffer is DirectBuffer)
				{
					Cleaner cleaner = ((DirectBuffer)buffer).Cleaner();
					cleaner.Clean();
				}
			}

			/// <summary>Linux only methods used for getOwner() implementation</summary>
			/// <exception cref="System.IO.IOException"/>
			private static long GetUIDforFDOwnerforOwner(FileDescriptor fd)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			private static string GetUserName(long uid)
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

				public const int SIfmt = 0xf000;

				public const int SIfifo = 0x1000;

				public const int SIfchr = 0x2000;

				public const int SIfdir = 0x4000;

				public const int SIfblk = 0x6000;

				public const int SIfreg = 0x8000;

				public const int SIflnk = 0xa000;

				public const int SIfsock = 0xc000;

				public const int SIfwht = 0xe000;

				public const int SIsuid = 0x800;

				public const int SIsgid = 0x400;

				public const int SIsvtx = 0x200;

				public const int SIrusr = 0x100;

				public const int SIwusr = 0x80;

				public const int SIxusr = 0x40;

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
					if (!Shell.Windows)
					{
						this.owner = owner;
					}
					else
					{
						this.owner = StripDomain(owner);
					}
					if (!Shell.Windows)
					{
						this.group = group;
					}
					else
					{
						this.group = StripDomain(group);
					}
					this.mode = mode;
				}

				public override string ToString()
				{
					return "Stat(owner='" + owner + "', group='" + group + "'" + ", mode=" + mode + ")";
				}

				public virtual string GetOwner()
				{
					return owner;
				}

				public virtual string GetGroup()
				{
					return group;
				}

				public virtual int GetMode()
				{
					return mode;
				}
			}

			/// <summary>Returns the file stat for a file descriptor.</summary>
			/// <param name="fd">file descriptor.</param>
			/// <returns>the file descriptor file stat.</returns>
			/// <exception cref="System.IO.IOException">thrown if there was an IO error while obtaining the file stat.
			/// 	</exception>
			public static NativeIO.POSIX.Stat GetFstat(FileDescriptor fd)
			{
				NativeIO.POSIX.Stat stat = null;
				if (!Shell.Windows)
				{
					stat = Fstat(fd);
					stat.owner = GetName(NativeIO.POSIX.IdCache.User, stat.ownerId);
					stat.group = GetName(NativeIO.POSIX.IdCache.Group, stat.groupId);
				}
				else
				{
					try
					{
						stat = Fstat(fd);
					}
					catch (NativeIOException nioe)
					{
						if (nioe.GetErrorCode() == 6)
						{
							throw new NativeIOException("The handle is invalid.", Errno.Ebadf);
						}
						else
						{
							Log.Warn(string.Format("NativeIO.getFstat error (%d): %s", nioe.GetErrorCode(), nioe
								.Message));
							throw new NativeIOException("Unknown error", Errno.Unknown);
						}
					}
				}
				return stat;
			}

			/// <exception cref="System.IO.IOException"/>
			private static string GetName(NativeIO.POSIX.IdCache domain, int id)
			{
				IDictionary<int, NativeIO.POSIX.CachedName> idNameCache = (domain == NativeIO.POSIX.IdCache
					.User) ? UserIdNameCache : GroupIdNameCache;
				string name;
				NativeIO.POSIX.CachedName cachedName = idNameCache[id];
				long now = Runtime.CurrentTimeMillis();
				if (cachedName != null && (cachedName.timestamp + cacheTimeout) > now)
				{
					name = cachedName.name;
				}
				else
				{
					name = (domain == NativeIO.POSIX.IdCache.User) ? GetUserName(id) : GetGroupName(id
						);
					if (Log.IsDebugEnabled())
					{
						string type = (domain == NativeIO.POSIX.IdCache.User) ? "UserName" : "GroupName";
						Log.Debug("Got " + type + " " + name + " for ID " + id + " from the native implementation"
							);
					}
					cachedName = new NativeIO.POSIX.CachedName(name, now);
					idNameCache[id] = cachedName;
				}
				return name;
			}

			/// <exception cref="System.IO.IOException"/>
			internal static string GetUserName(int uid)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal static string GetGroupName(int uid)
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

			private static readonly IDictionary<int, NativeIO.POSIX.CachedName> UserIdNameCache
				 = new ConcurrentHashMap<int, NativeIO.POSIX.CachedName>();

			private static readonly IDictionary<int, NativeIO.POSIX.CachedName> GroupIdNameCache
				 = new ConcurrentHashMap<int, NativeIO.POSIX.CachedName>();

			private enum IdCache
			{
				User,
				Group
			}

			public const int MmapProtRead = unchecked((int)(0x1));

			public const int MmapProtWrite = unchecked((int)(0x2));

			public const int MmapProtExec = unchecked((int)(0x4));

			/// <exception cref="System.IO.IOException"/>
			public static long Mmap(FileDescriptor fd, int prot, bool shared, long length)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public static void Munmap(long addr, long length)
			{
			}
		}

		private static bool workaroundNonThreadSafePasswdCalls = false;

		public class Windows
		{
			public const long GenericRead = unchecked((long)(0x80000000L));

			public const long GenericWrite = unchecked((long)(0x40000000L));

			public const long FileShareRead = unchecked((long)(0x00000001L));

			public const long FileShareWrite = unchecked((long)(0x00000002L));

			public const long FileShareDelete = unchecked((long)(0x00000004L));

			public const long CreateNew = 1;

			public const long CreateAlways = 2;

			public const long OpenExisting = 3;

			public const long OpenAlways = 4;

			public const long TruncateExisting = 5;

			public const long FileBegin = 0;

			public const long FileCurrent = 1;

			public const long FileEnd = 2;

			public const long FileAttributeNormal = unchecked((long)(0x00000080L));

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
			public static void CreateDirectoryWithMode(FilePath path, int mode)
			{
				CreateDirectoryWithMode0(path.GetAbsolutePath(), mode);
			}

			/// <summary>Wrapper around CreateDirectory() on Windows</summary>
			/// <exception cref="Org.Apache.Hadoop.IO.Nativeio.NativeIOException"/>
			private static void CreateDirectoryWithMode0(string path, int mode)
			{
			}

			/// <summary>Wrapper around CreateFile() on Windows</summary>
			/// <exception cref="System.IO.IOException"/>
			public static FileDescriptor CreateFile(string path, long desiredAccess, long shareMode
				, long creationDisposition)
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
			/// <see cref="System.IO.FileOutputStream"/>
			/// .  The file is opened for truncate or
			/// append, the sharing mode allows other readers and writers, and paths
			/// longer than MAX_PATH are supported.  (See io_util_md.c in the JDK.)
			/// </remarks>
			/// <param name="path">file to create</param>
			/// <param name="append">if true, then open file for append</param>
			/// <param name="mode">permissions of new directory</param>
			/// <returns>FileOutputStream of opened file</returns>
			/// <exception cref="System.IO.IOException">if there is an I/O error</exception>
			public static FileOutputStream CreateFileOutputStreamWithMode(FilePath path, bool
				 append, int mode)
			{
				long desiredAccess = GenericWrite;
				long shareMode = FileShareRead | FileShareWrite;
				long creationDisposition = append ? OpenAlways : CreateAlways;
				return new FileOutputStream(CreateFileWithMode0(path.GetAbsolutePath(), desiredAccess
					, shareMode, creationDisposition, mode));
			}

			/// <summary>Wrapper around CreateFile() with security descriptor on Windows</summary>
			/// <exception cref="Org.Apache.Hadoop.IO.Nativeio.NativeIOException"/>
			private static FileDescriptor CreateFileWithMode0(string path, long desiredAccess
				, long shareMode, long creationDisposition, int mode)
			{
			}

			/// <summary>Wrapper around SetFilePointer() on Windows</summary>
			/// <exception cref="System.IO.IOException"/>
			public static long SetFilePointer(FileDescriptor fd, long distanceToMove, long moveMethod
				)
			{
			}

			/// <summary>Windows only methods used for getOwner() implementation</summary>
			/// <exception cref="System.IO.IOException"/>
			private static string GetOwner(FileDescriptor fd)
			{
			}

			/// <summary>Supported list of Windows access right flags</summary>
			[System.Serializable]
			public sealed class AccessRight
			{
				public static readonly NativeIO.Windows.AccessRight AccessRead = new NativeIO.Windows.AccessRight
					(unchecked((int)(0x0001)));

				public static readonly NativeIO.Windows.AccessRight AccessWrite = new NativeIO.Windows.AccessRight
					(unchecked((int)(0x0002)));

				public static readonly NativeIO.Windows.AccessRight AccessExecute = new NativeIO.Windows.AccessRight
					(unchecked((int)(0x0020)));

				private readonly int accessRight;

				internal AccessRight(int access)
				{
					// FILE_READ_DATA
					// FILE_WRITE_DATA
					// FILE_EXECUTE
					NativeIO.Windows.AccessRight.accessRight = access;
				}

				public int AccessRight()
				{
					return NativeIO.Windows.AccessRight.accessRight;
				}
			}

			/// <summary>
			/// Windows only method used to check if the current process has requested
			/// access rights on the given path.
			/// </summary>
			private static bool Access0(string path, int requestedAccess)
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
			public static bool Access(string path, NativeIO.Windows.AccessRight desiredAccess
				)
			{
				return Access0(path, desiredAccess.AccessRight());
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
			/// <seealso cref="POSIX.Mlock(Sharpen.ByteBuffer, long)"/>
			public static void ExtendWorkingSetSize(long delta)
			{
			}

			static Windows()
			{
				if (NativeCodeLoader.IsNativeCodeLoaded())
				{
					try
					{
						InitNative();
						nativeLoaded = true;
					}
					catch (Exception t)
					{
						// This can happen if the user has an older version of libhadoop.so
						// installed - in this case we can continue without native IO
						// after warning
						PerformanceAdvisory.Log.Debug("Unable to initialize NativeIO libraries", t);
					}
				}
			}
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(NativeIO));

		private static bool nativeLoaded = false;

		static NativeIO()
		{
			if (NativeCodeLoader.IsNativeCodeLoaded())
			{
				try
				{
					InitNative();
					nativeLoaded = true;
				}
				catch (Exception t)
				{
					// This can happen if the user has an older version of libhadoop.so
					// installed - in this case we can continue without native IO
					// after warning
					PerformanceAdvisory.Log.Debug("Unable to initialize NativeIO libraries", t);
				}
			}
		}

		/// <summary>Return true if the JNI-based native IO extensions are available.</summary>
		public static bool IsAvailable()
		{
			return NativeCodeLoader.IsNativeCodeLoaded() && nativeLoaded;
		}

		/// <summary>Initialize the JNI method ID and class ID cache</summary>
		private static void InitNative()
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
		internal static long GetMemlockLimit()
		{
			return IsAvailable() ? GetMemlockLimit0() : 0;
		}

		private static long GetMemlockLimit0()
		{
		}

		/// <returns>the operating system's page size.</returns>
		internal static long GetOperatingSystemPageSize()
		{
			try
			{
				FieldInfo f = Sharpen.Runtime.GetDeclaredField(typeof(Unsafe), "theUnsafe");
				Unsafe @unsafe = (Unsafe)f.GetValue(null);
				return @unsafe.PageSize();
			}
			catch (Exception e)
			{
				Log.Warn("Unable to get operating system page size.  Guessing 4096.", e);
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

		private static readonly IDictionary<long, NativeIO.CachedUid> uidCache = new ConcurrentHashMap
			<long, NativeIO.CachedUid>();

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
		private static string StripDomain(string name)
		{
			int i = name.IndexOf('\\');
			if (i != -1)
			{
				name = Sharpen.Runtime.Substring(name, i + 1);
			}
			return name;
		}

		/// <exception cref="System.IO.IOException"/>
		public static string GetOwner(FileDescriptor fd)
		{
			EnsureInitialized();
			if (Shell.Windows)
			{
				string owner = NativeIO.Windows.GetOwner(fd);
				owner = StripDomain(owner);
				return owner;
			}
			else
			{
				long uid = NativeIO.POSIX.GetUIDforFDOwnerforOwner(fd);
				NativeIO.CachedUid cUid = uidCache[uid];
				long now = Runtime.CurrentTimeMillis();
				if (cUid != null && (cUid.timestamp + cacheTimeout) > now)
				{
					return cUid.username;
				}
				string user = NativeIO.POSIX.GetUserName(uid);
				Log.Info("Got UserName " + user + " for UID " + uid + " from the native implementation"
					);
				cUid = new NativeIO.CachedUid(user, now);
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
		public static FileInputStream GetShareDeleteFileInputStream(FilePath f)
		{
			if (!Shell.Windows)
			{
				// On Linux the default FileInputStream shares delete permission
				// on the file opened.
				//
				return new FileInputStream(f);
			}
			else
			{
				// Use Windows native interface to create a FileInputStream that
				// shares delete permission on the file opened.
				//
				FileDescriptor fd = NativeIO.Windows.CreateFile(f.GetAbsolutePath(), NativeIO.Windows
					.GenericRead, NativeIO.Windows.FileShareRead | NativeIO.Windows.FileShareWrite |
					 NativeIO.Windows.FileShareDelete, NativeIO.Windows.OpenExisting);
				return new FileInputStream(fd);
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
		public static FileInputStream GetShareDeleteFileInputStream(FilePath f, long seekOffset
			)
		{
			if (!Shell.Windows)
			{
				RandomAccessFile rf = new RandomAccessFile(f, "r");
				if (seekOffset > 0)
				{
					rf.Seek(seekOffset);
				}
				return new FileInputStream(rf.GetFD());
			}
			else
			{
				// Use Windows native interface to create a FileInputStream that
				// shares delete permission on the file opened, and set it to the
				// given offset.
				//
				FileDescriptor fd = NativeIO.Windows.CreateFile(f.GetAbsolutePath(), NativeIO.Windows
					.GenericRead, NativeIO.Windows.FileShareRead | NativeIO.Windows.FileShareWrite |
					 NativeIO.Windows.FileShareDelete, NativeIO.Windows.OpenExisting);
				if (seekOffset > 0)
				{
					NativeIO.Windows.SetFilePointer(fd, seekOffset, NativeIO.Windows.FileBegin);
				}
				return new FileInputStream(fd);
			}
		}

		/// <summary>Create the specified File for write access, ensuring that it does not exist.
		/// 	</summary>
		/// <param name="f">the file that we want to create</param>
		/// <param name="permissions">we want to have on the file (if security is enabled)</param>
		/// <exception cref="Org.Apache.Hadoop.IO.SecureIOUtils.AlreadyExistsException">if the file already exists
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if any other error occurred</exception>
		public static FileOutputStream GetCreateForWriteFileOutputStream(FilePath f, int 
			permissions)
		{
			if (!Shell.Windows)
			{
				// Use the native wrapper around open(2)
				try
				{
					FileDescriptor fd = NativeIO.POSIX.Open(f.GetAbsolutePath(), NativeIO.POSIX.OWronly
						 | NativeIO.POSIX.OCreat | NativeIO.POSIX.OExcl, permissions);
					return new FileOutputStream(fd);
				}
				catch (NativeIOException nioe)
				{
					if (nioe.GetErrno() == Errno.Eexist)
					{
						throw new SecureIOUtils.AlreadyExistsException(nioe);
					}
					throw;
				}
			}
			else
			{
				// Use the Windows native APIs to create equivalent FileOutputStream
				try
				{
					FileDescriptor fd = NativeIO.Windows.CreateFile(f.GetCanonicalPath(), NativeIO.Windows
						.GenericWrite, NativeIO.Windows.FileShareDelete | NativeIO.Windows.FileShareRead
						 | NativeIO.Windows.FileShareWrite, NativeIO.Windows.CreateNew);
					NativeIO.POSIX.Chmod(f.GetCanonicalPath(), permissions);
					return new FileOutputStream(fd);
				}
				catch (NativeIOException nioe)
				{
					if (nioe.GetErrorCode() == 80)
					{
						// ERROR_FILE_EXISTS
						// 80 (0x50)
						// The file exists
						throw new SecureIOUtils.AlreadyExistsException(nioe);
					}
					throw;
				}
			}
		}

		private static void EnsureInitialized()
		{
			lock (typeof(NativeIO))
			{
				if (!initialized)
				{
					cacheTimeout = new Configuration().GetLong("hadoop.security.uid.cache.secs", 4 * 
						60 * 60) * 1000;
					Log.Info("Initialized cache for UID to User mapping with a cache" + " timeout of "
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
		public static void RenameTo(FilePath src, FilePath dst)
		{
			if (!nativeLoaded)
			{
				if (!src.RenameTo(dst))
				{
					throw new IOException("renameTo(src=" + src + ", dst=" + dst + ") failed.");
				}
			}
			else
			{
				RenameTo0(src.GetAbsolutePath(), dst.GetAbsolutePath());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Link(FilePath src, FilePath dst)
		{
			if (!nativeLoaded)
			{
				HardLink.CreateHardLink(src, dst);
			}
			else
			{
				Link0(src.GetAbsolutePath(), dst.GetAbsolutePath());
			}
		}

		/// <summary>A version of renameTo that throws a descriptive exception when it fails.
		/// 	</summary>
		/// <param name="src">The source path</param>
		/// <param name="dst">The destination path</param>
		/// <exception cref="NativeIOException">On failure.</exception>
		/// <exception cref="Org.Apache.Hadoop.IO.Nativeio.NativeIOException"/>
		private static void RenameTo0(string src, string dst)
		{
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Nativeio.NativeIOException"/>
		private static void Link0(string src, string dst)
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
		public static void CopyFileUnbuffered(FilePath src, FilePath dst)
		{
			if (nativeLoaded && Shell.Windows)
			{
				CopyFileUnbuffered0(src.GetAbsolutePath(), dst.GetAbsolutePath());
			}
			else
			{
				FileInputStream fis = null;
				FileOutputStream fos = null;
				FileChannel input = null;
				FileChannel output = null;
				try
				{
					fis = new FileInputStream(src);
					fos = new FileOutputStream(dst);
					input = fis.GetChannel();
					output = fos.GetChannel();
					long remaining = input.Size();
					long position = 0;
					long transferred = 0;
					while (remaining > 0)
					{
						transferred = input.TransferTo(position, remaining, output);
						remaining -= transferred;
						position += transferred;
					}
				}
				finally
				{
					IOUtils.Cleanup(Log, output);
					IOUtils.Cleanup(Log, fos);
					IOUtils.Cleanup(Log, input);
					IOUtils.Cleanup(Log, fis);
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Nativeio.NativeIOException"/>
		private static void CopyFileUnbuffered0(string src, string dst)
		{
		}
	}
}
