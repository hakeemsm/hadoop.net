

namespace Org.Apache.Hadoop.Nfs.Nfs3
{
	/// <summary>Some constants for NFSv3</summary>
	public class Nfs3Constant
	{
		public const int SunRpcbind = 111;

		public const int Program = 100003;

		public const int Version = 3;

		[System.Serializable]
		public sealed class NFSPROC3
		{
			public static readonly Nfs3Constant.NFSPROC3 Null = new Nfs3Constant.NFSPROC3();

			public static readonly Nfs3Constant.NFSPROC3 Getattr = new Nfs3Constant.NFSPROC3(
				);

			public static readonly Nfs3Constant.NFSPROC3 Setattr = new Nfs3Constant.NFSPROC3(
				);

			public static readonly Nfs3Constant.NFSPROC3 Lookup = new Nfs3Constant.NFSPROC3();

			public static readonly Nfs3Constant.NFSPROC3 Access = new Nfs3Constant.NFSPROC3();

			public static readonly Nfs3Constant.NFSPROC3 Readlink = new Nfs3Constant.NFSPROC3
				();

			public static readonly Nfs3Constant.NFSPROC3 Read = new Nfs3Constant.NFSPROC3();

			public static readonly Nfs3Constant.NFSPROC3 Write = new Nfs3Constant.NFSPROC3();

			public static readonly Nfs3Constant.NFSPROC3 Create = new Nfs3Constant.NFSPROC3(false
				);

			public static readonly Nfs3Constant.NFSPROC3 Mkdir = new Nfs3Constant.NFSPROC3(false
				);

			public static readonly Nfs3Constant.NFSPROC3 Symlink = new Nfs3Constant.NFSPROC3(
				false);

			public static readonly Nfs3Constant.NFSPROC3 Mknod = new Nfs3Constant.NFSPROC3(false
				);

			public static readonly Nfs3Constant.NFSPROC3 Remove = new Nfs3Constant.NFSPROC3(false
				);

			public static readonly Nfs3Constant.NFSPROC3 Rmdir = new Nfs3Constant.NFSPROC3(false
				);

			public static readonly Nfs3Constant.NFSPROC3 Rename = new Nfs3Constant.NFSPROC3(false
				);

			public static readonly Nfs3Constant.NFSPROC3 Link = new Nfs3Constant.NFSPROC3(false
				);

			public static readonly Nfs3Constant.NFSPROC3 Readdir = new Nfs3Constant.NFSPROC3(
				);

			public static readonly Nfs3Constant.NFSPROC3 Readdirplus = new Nfs3Constant.NFSPROC3
				();

			public static readonly Nfs3Constant.NFSPROC3 Fsstat = new Nfs3Constant.NFSPROC3();

			public static readonly Nfs3Constant.NFSPROC3 Fsinfo = new Nfs3Constant.NFSPROC3();

			public static readonly Nfs3Constant.NFSPROC3 Pathconf = new Nfs3Constant.NFSPROC3
				();

			public static readonly Nfs3Constant.NFSPROC3 Commit = new Nfs3Constant.NFSPROC3();

			private readonly bool isIdempotent;

			private NFSPROC3(bool isIdempotent)
			{
				// The local rpcbind/portmapper port.
				// The RPC program number for NFS.
				// The program version number that this server implements.
				// The procedures
				// the order of the values below are significant.
				this.isIdempotent = isIdempotent;
			}

			private NFSPROC3()
				: this(true)
			{
			}

			public bool IsIdempotent()
			{
				return Nfs3Constant.NFSPROC3.isIdempotent;
			}

			/// <returns>the int value representing the procedure.</returns>
			public int GetValue()
			{
				return Ordinal();
			}

			/// <returns>the procedure corresponding to the value.</returns>
			public static Nfs3Constant.NFSPROC3 FromValue(int value)
			{
				if (value < 0 || value >= Values().Length)
				{
					return null;
				}
				return Values()[value];
			}
		}

		public const int Nfs3Fhsize = 64;

		public const int Nfs3Cookieverfsize = 8;

		public const int Nfs3Createverfsize = 8;

		public const int Nfs3Writeverfsize = 8;

		/// <summary>Access call request mode</summary>
		public const int AccessModeRead = unchecked((int)(0x04));

		public const int AccessModeWrite = unchecked((int)(0x02));

		public const int AccessModeExecute = unchecked((int)(0x01));

		/// <summary>Access call response rights</summary>
		public const int Access3Read = unchecked((int)(0x0001));

		public const int Access3Lookup = unchecked((int)(0x0002));

		public const int Access3Modify = unchecked((int)(0x0004));

		public const int Access3Extend = unchecked((int)(0x0008));

		public const int Access3Delete = unchecked((int)(0x0010));

		public const int Access3Execute = unchecked((int)(0x0020));

		/// <summary>File and directory attribute mode bits</summary>
		public const int ModeSIsuid = unchecked((int)(0x00800));

		public const int ModeSIsgid = unchecked((int)(0x00400));

		public const int ModeSIsvtx = unchecked((int)(0x00200));

		public const int ModeSIrusr = unchecked((int)(0x00100));

		public const int ModeSIwusr = unchecked((int)(0x00080));

		public const int ModeSIxusr = unchecked((int)(0x00040));

		public const int ModeSIrgrp = unchecked((int)(0x00020));

		public const int ModeSIwgrp = unchecked((int)(0x00010));

		public const int ModeSIxgrp = unchecked((int)(0x00008));

		public const int ModeSIroth = unchecked((int)(0x00004));

		public const int ModeSIwoth = unchecked((int)(0x00002));

		public const int ModeSIxoth = unchecked((int)(0x00001));

		public const int ModeAll = ModeSIsuid | ModeSIsgid | ModeSIsvtx | ModeSIsvtx | ModeSIrusr
			 | ModeSIrusr | ModeSIwusr | ModeSIxusr | ModeSIrgrp | ModeSIwgrp | ModeSIxgrp |
			 ModeSIroth | ModeSIwoth | ModeSIxoth;

		/// <summary>Write call flavors</summary>
		[System.Serializable]
		public sealed class WriteStableHow
		{
			public static readonly Nfs3Constant.WriteStableHow Unstable = new Nfs3Constant.WriteStableHow
				();

			public static readonly Nfs3Constant.WriteStableHow DataSync = new Nfs3Constant.WriteStableHow
				();

			public static readonly Nfs3Constant.WriteStableHow FileSync = new Nfs3Constant.WriteStableHow
				();

			// The maximum size in bytes of the opaque file handle.
			// The byte size of cookie verifier passed by READDIR and READDIRPLUS.
			// The size in bytes of the opaque verifier used for exclusive CREATE.
			// The size in bytes of the opaque verifier used for asynchronous WRITE.
			// File access mode
			// Read data from file or read a directory.
			// Look up a name in a directory (no meaning for non-directory objects).
			// Rewrite existing file data or modify existing directory entries.
			// Write new data or add directory entries.
			// Delete an existing directory entry.
			// Execute file (no meaning for a directory).
			// Set user ID on execution.
			// Set group ID on execution.
			// Save swapped text (not defined in POSIX).
			// Read permission for owner.
			// Write permission for owner.
			// Execute permission for owner on a file. Or lookup (search) permission for
			// owner in directory.
			// Read permission for group.
			// Write permission for group.
			// Execute permission for group on a file. Or lookup (search) permission for
			// group in directory.
			// Read permission for others.
			// Write permission for others.
			// Execute permission for others on a file. Or lookup (search) permission for
			// others in directory.
			// the order of the values below are significant.
			public int GetValue()
			{
				return Ordinal();
			}

			public static Nfs3Constant.WriteStableHow FromValue(int id)
			{
				return Values()[id];
			}
		}

		/// <summary>
		/// This is a cookie that the client can use to determine whether the server
		/// has changed state between a call to WRITE and a subsequent call to either
		/// WRITE or COMMIT.
		/// </summary>
		/// <remarks>
		/// This is a cookie that the client can use to determine whether the server
		/// has changed state between a call to WRITE and a subsequent call to either
		/// WRITE or COMMIT. This cookie must be consistent during a single instance of
		/// the NFS version 3 protocol service and must be unique between instances of
		/// the NFS version 3 protocol server, where uncommitted data may be lost.
		/// </remarks>
		public static readonly long WriteCommitVerf = Runtime.CurrentTimeMillis();

		/// <summary>FileSystemProperties</summary>
		public const int Fsf3Link = unchecked((int)(0x0001));

		public const int Fsf3Symlink = unchecked((int)(0x0002));

		public const int Fsf3Homogeneous = unchecked((int)(0x0008));

		public const int Fsf3Cansettime = unchecked((int)(0x0010));

		/// <summary>Create options</summary>
		public const int CreateUnchecked = 0;

		public const int CreateGuarded = 1;

		public const int CreateExclusive = 2;

		/// <summary>Size for nfs exports cache</summary>
		public const string NfsExportsCacheSizeKey = "nfs.exports.cache.size";

		public const int NfsExportsCacheSizeDefault = 512;

		/// <summary>Expiration time for nfs exports cache entry</summary>
		public const string NfsExportsCacheExpirytimeMillisKey = "nfs.exports.cache.expirytime.millis";

		public const long NfsExportsCacheExpirytimeMillisDefault = 15 * 60 * 1000;
		// 15 min
	}
}
