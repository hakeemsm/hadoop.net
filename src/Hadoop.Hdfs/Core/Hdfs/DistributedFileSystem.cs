using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Implementation of the abstract FileSystem for the DFS system.</summary>
	/// <remarks>
	/// Implementation of the abstract FileSystem for the DFS system.
	/// This object is the way end-user code interacts with a Hadoop
	/// DistributedFileSystem.
	/// </remarks>
	public class DistributedFileSystem : FileSystem
	{
		private Path workingDir;

		private URI uri;

		private string homeDirPrefix = DFSConfigKeys.DfsUserHomeDirPrefixDefault;

		internal DFSClient dfs;

		private bool verifyChecksum = true;

		static DistributedFileSystem()
		{
			HdfsConfiguration.Init();
		}

		public DistributedFileSystem()
		{
		}

		/// <summary>Return the protocol scheme for the FileSystem.</summary>
		/// <remarks>
		/// Return the protocol scheme for the FileSystem.
		/// <p/>
		/// </remarks>
		/// <returns><code>hdfs</code></returns>
		public override string GetScheme()
		{
			return HdfsConstants.HdfsUriScheme;
		}

		public override URI GetUri()
		{
			return uri;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Initialize(URI uri, Configuration conf)
		{
			base.Initialize(uri, conf);
			SetConf(conf);
			string host = uri.GetHost();
			if (host == null)
			{
				throw new IOException("Incomplete HDFS URI, no host: " + uri);
			}
			homeDirPrefix = conf.Get(DFSConfigKeys.DfsUserHomeDirPrefixKey, DFSConfigKeys.DfsUserHomeDirPrefixDefault
				);
			this.dfs = new DFSClient(uri, conf, statistics);
			this.uri = URI.Create(uri.GetScheme() + "://" + uri.GetAuthority());
			this.workingDir = GetHomeDirectory();
		}

		public override Path GetWorkingDirectory()
		{
			return workingDir;
		}

		public override long GetDefaultBlockSize()
		{
			return dfs.GetDefaultBlockSize();
		}

		public override short GetDefaultReplication()
		{
			return dfs.GetDefaultReplication();
		}

		public override void SetWorkingDirectory(Path dir)
		{
			string result = FixRelativePart(dir).ToUri().GetPath();
			if (!DFSUtil.IsValidName(result))
			{
				throw new ArgumentException("Invalid DFS directory name " + result);
			}
			workingDir = FixRelativePart(dir);
		}

		public override Path GetHomeDirectory()
		{
			return MakeQualified(new Path(homeDirPrefix + "/" + dfs.ugi.GetShortUserName()));
		}

		/// <summary>
		/// Checks that the passed URI belongs to this filesystem and returns
		/// just the path component.
		/// </summary>
		/// <remarks>
		/// Checks that the passed URI belongs to this filesystem and returns
		/// just the path component. Expects a URI with an absolute path.
		/// </remarks>
		/// <param name="file">URI with absolute path</param>
		/// <returns>path component of {file}</returns>
		/// <exception cref="System.ArgumentException">if URI does not belong to this DFS</exception>
		private string GetPathName(Path file)
		{
			CheckPath(file);
			string result = file.ToUri().GetPath();
			if (!DFSUtil.IsValidName(result))
			{
				throw new ArgumentException("Pathname " + result + " from " + file + " is not a valid DFS filename."
					);
			}
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		public override BlockLocation[] GetFileBlockLocations(FileStatus file, long start
			, long len)
		{
			if (file == null)
			{
				return null;
			}
			return GetFileBlockLocations(file.GetPath(), start, len);
		}

		/// <exception cref="System.IO.IOException"/>
		public override BlockLocation[] GetFileBlockLocations(Path p, long start, long len
			)
		{
			statistics.IncrementReadOps(1);
			Path absF = FixRelativePart(p);
			return new _FileSystemLinkResolver_217(this, start, len).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_217 : FileSystemLinkResolver<BlockLocation
			[]>
		{
			public _FileSystemLinkResolver_217(DistributedFileSystem _enclosing, long start, 
				long len)
			{
				this._enclosing = _enclosing;
				this.start = start;
				this.len = len;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override BlockLocation[] DoCall(Path p)
			{
				return this._enclosing.dfs.GetBlockLocations(this._enclosing.GetPathName(p), start
					, len);
			}

			/// <exception cref="System.IO.IOException"/>
			public override BlockLocation[] Next(FileSystem fs, Path p)
			{
				return fs.GetFileBlockLocations(p, start, len);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly long start;

			private readonly long len;
		}

		/// <summary>Used to query storage location information for a list of blocks.</summary>
		/// <remarks>
		/// Used to query storage location information for a list of blocks. This list
		/// of blocks is normally constructed via a series of calls to
		/// <see cref="GetFileBlockLocations(Org.Apache.Hadoop.FS.Path, long, long)"/>
		/// to
		/// get the blocks for ranges of a file.
		/// The returned array of
		/// <see cref="Org.Apache.Hadoop.FS.BlockStorageLocation"/>
		/// augments
		/// <see cref="Org.Apache.Hadoop.FS.BlockLocation"/>
		/// with a
		/// <see cref="Org.Apache.Hadoop.FS.VolumeId"/>
		/// per block replica. The
		/// VolumeId specifies the volume on the datanode on which the replica resides.
		/// The VolumeId associated with a replica may be null because volume
		/// information can be unavailable if the corresponding datanode is down or
		/// if the requested block is not found.
		/// This API is unstable, and datanode-side support is disabled by default. It
		/// can be enabled by setting "dfs.datanode.hdfs-blocks-metadata.enabled" to
		/// true.
		/// </remarks>
		/// <param name="blocks">List of target BlockLocations to query volume location information
		/// 	</param>
		/// <returns>
		/// volumeBlockLocations Augmented array of
		/// <see cref="Org.Apache.Hadoop.FS.BlockStorageLocation"/>
		/// s containing additional volume location
		/// information for each replica of each block.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Security.Token.Block.InvalidBlockTokenException
		/// 	"/>
		[InterfaceStability.Unstable]
		public virtual BlockStorageLocation[] GetFileBlockStorageLocations(IList<BlockLocation
			> blocks)
		{
			return dfs.GetBlockStorageLocations(blocks);
		}

		public override void SetVerifyChecksum(bool verifyChecksum)
		{
			this.verifyChecksum = verifyChecksum;
		}

		/// <summary>Start the lease recovery of a file</summary>
		/// <param name="f">a file</param>
		/// <returns>true if the file is already closed</returns>
		/// <exception cref="System.IO.IOException">if an error occurs</exception>
		public virtual bool RecoverLease(Path f)
		{
			Path absF = FixRelativePart(f);
			return new _FileSystemLinkResolver_275(this, f).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_275 : FileSystemLinkResolver<bool>
		{
			public _FileSystemLinkResolver_275(DistributedFileSystem _enclosing, Path f)
			{
				this._enclosing = _enclosing;
				this.f = f;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override bool DoCall(Path p)
			{
				return this._enclosing.dfs.RecoverLease(this._enclosing.GetPathName(p));
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Next(FileSystem fs, Path p)
			{
				if (fs is Org.Apache.Hadoop.Hdfs.DistributedFileSystem)
				{
					Org.Apache.Hadoop.Hdfs.DistributedFileSystem myDfs = (Org.Apache.Hadoop.Hdfs.DistributedFileSystem
						)fs;
					return myDfs.RecoverLease(p);
				}
				throw new NotSupportedException("Cannot recoverLease through" + " a symlink to a non-DistributedFileSystem: "
					 + f + " -> " + p);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly Path f;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataInputStream Open(Path f, int bufferSize)
		{
			statistics.IncrementReadOps(1);
			Path absF = FixRelativePart(f);
			return new _FileSystemLinkResolver_299(this, bufferSize).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_299 : FileSystemLinkResolver<FSDataInputStream
			>
		{
			public _FileSystemLinkResolver_299(DistributedFileSystem _enclosing, int bufferSize
				)
			{
				this._enclosing = _enclosing;
				this.bufferSize = bufferSize;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override FSDataInputStream DoCall(Path p)
			{
				DFSInputStream dfsis = this._enclosing.dfs.Open(this._enclosing.GetPathName(p), bufferSize
					, this._enclosing.verifyChecksum);
				return this._enclosing.dfs.CreateWrappedInputStream(dfsis);
			}

			/// <exception cref="System.IO.IOException"/>
			public override FSDataInputStream Next(FileSystem fs, Path p)
			{
				return fs.Open(p, bufferSize);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly int bufferSize;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Append(Path f, int bufferSize, Progressable progress
			)
		{
			return Append(f, EnumSet.Of(CreateFlag.Append), bufferSize, progress);
		}

		/// <summary>Append to an existing file (optional operation).</summary>
		/// <param name="f">the existing file to be appended.</param>
		/// <param name="flag">
		/// Flags for the Append operation. CreateFlag.APPEND is mandatory
		/// to be present.
		/// </param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <param name="progress">for reporting progress if it is not null.</param>
		/// <returns>
		/// Returns instance of
		/// <see cref="Org.Apache.Hadoop.FS.FSDataOutputStream"/>
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual FSDataOutputStream Append(Path f, EnumSet<CreateFlag> flag, int bufferSize
			, Progressable progress)
		{
			statistics.IncrementWriteOps(1);
			Path absF = FixRelativePart(f);
			return new _FileSystemLinkResolver_336(this, bufferSize, flag, progress).Resolve(
				this, absF);
		}

		private sealed class _FileSystemLinkResolver_336 : FileSystemLinkResolver<FSDataOutputStream
			>
		{
			public _FileSystemLinkResolver_336(DistributedFileSystem _enclosing, int bufferSize
				, EnumSet<CreateFlag> flag, Progressable progress)
			{
				this._enclosing = _enclosing;
				this.bufferSize = bufferSize;
				this.flag = flag;
				this.progress = progress;
			}

			/// <exception cref="System.IO.IOException"/>
			public override FSDataOutputStream DoCall(Path p)
			{
				return this._enclosing.dfs.Append(this._enclosing.GetPathName(p), bufferSize, flag
					, progress, this._enclosing.statistics);
			}

			/// <exception cref="System.IO.IOException"/>
			public override FSDataOutputStream Next(FileSystem fs, Path p)
			{
				return fs.Append(p, bufferSize);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly int bufferSize;

			private readonly EnumSet<CreateFlag> flag;

			private readonly Progressable progress;
		}

		/// <summary>Append to an existing file (optional operation).</summary>
		/// <param name="f">the existing file to be appended.</param>
		/// <param name="flag">
		/// Flags for the Append operation. CreateFlag.APPEND is mandatory
		/// to be present.
		/// </param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <param name="progress">for reporting progress if it is not null.</param>
		/// <param name="favoredNodes">Favored nodes for new blocks</param>
		/// <returns>
		/// Returns instance of
		/// <see cref="Org.Apache.Hadoop.FS.FSDataOutputStream"/>
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual FSDataOutputStream Append(Path f, EnumSet<CreateFlag> flag, int bufferSize
			, Progressable progress, IPEndPoint[] favoredNodes)
		{
			statistics.IncrementWriteOps(1);
			Path absF = FixRelativePart(f);
			return new _FileSystemLinkResolver_368(this, bufferSize, flag, progress, favoredNodes
				).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_368 : FileSystemLinkResolver<FSDataOutputStream
			>
		{
			public _FileSystemLinkResolver_368(DistributedFileSystem _enclosing, int bufferSize
				, EnumSet<CreateFlag> flag, Progressable progress, IPEndPoint[] favoredNodes)
			{
				this._enclosing = _enclosing;
				this.bufferSize = bufferSize;
				this.flag = flag;
				this.progress = progress;
				this.favoredNodes = favoredNodes;
			}

			/// <exception cref="System.IO.IOException"/>
			public override FSDataOutputStream DoCall(Path p)
			{
				return this._enclosing.dfs.Append(this._enclosing.GetPathName(p), bufferSize, flag
					, progress, this._enclosing.statistics, favoredNodes);
			}

			/// <exception cref="System.IO.IOException"/>
			public override FSDataOutputStream Next(FileSystem fs, Path p)
			{
				return fs.Append(p, bufferSize);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly int bufferSize;

			private readonly EnumSet<CreateFlag> flag;

			private readonly Progressable progress;

			private readonly IPEndPoint[] favoredNodes;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
			, int bufferSize, short replication, long blockSize, Progressable progress)
		{
			return this.Create(f, permission, overwrite ? EnumSet.Of(CreateFlag.Create, CreateFlag
				.Overwrite) : EnumSet.Of(CreateFlag.Create), bufferSize, replication, blockSize, 
				progress, null);
		}

		/// <summary>
		/// Same as
		/// <see cref="Create(Org.Apache.Hadoop.FS.Path, Org.Apache.Hadoop.FS.Permission.FsPermission, bool, int, short, long, Org.Apache.Hadoop.Util.Progressable)
		/// 	"/>
		/// with the addition of favoredNodes that is a hint to
		/// where the namenode should place the file blocks.
		/// The favored nodes hint is not persisted in HDFS. Hence it may be honored
		/// at the creation time only. And with favored nodes, blocks will be pinned
		/// on the datanodes to prevent balancing move the block. HDFS could move the
		/// blocks during replication, to move the blocks from favored nodes. A value
		/// of null means no favored nodes for this create
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual HdfsDataOutputStream Create(Path f, FsPermission permission, bool 
			overwrite, int bufferSize, short replication, long blockSize, Progressable progress
			, IPEndPoint[] favoredNodes)
		{
			statistics.IncrementWriteOps(1);
			Path absF = FixRelativePart(f);
			return new _FileSystemLinkResolver_411(this, f, permission, overwrite, replication
				, blockSize, progress, bufferSize, favoredNodes).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_411 : FileSystemLinkResolver<HdfsDataOutputStream
			>
		{
			public _FileSystemLinkResolver_411(DistributedFileSystem _enclosing, Path f, FsPermission
				 permission, bool overwrite, short replication, long blockSize, Progressable progress
				, int bufferSize, IPEndPoint[] favoredNodes)
			{
				this._enclosing = _enclosing;
				this.f = f;
				this.permission = permission;
				this.overwrite = overwrite;
				this.replication = replication;
				this.blockSize = blockSize;
				this.progress = progress;
				this.bufferSize = bufferSize;
				this.favoredNodes = favoredNodes;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override HdfsDataOutputStream DoCall(Path p)
			{
				DFSOutputStream @out = this._enclosing.dfs.Create(this._enclosing.GetPathName(f), 
					permission, overwrite ? EnumSet.Of(CreateFlag.Create, CreateFlag.Overwrite) : EnumSet
					.Of(CreateFlag.Create), true, replication, blockSize, progress, bufferSize, null
					, favoredNodes);
				return this._enclosing.dfs.CreateWrappedOutputStream(@out, this._enclosing.statistics
					);
			}

			/// <exception cref="System.IO.IOException"/>
			public override HdfsDataOutputStream Next(FileSystem fs, Path p)
			{
				if (fs is Org.Apache.Hadoop.Hdfs.DistributedFileSystem)
				{
					Org.Apache.Hadoop.Hdfs.DistributedFileSystem myDfs = (Org.Apache.Hadoop.Hdfs.DistributedFileSystem
						)fs;
					return myDfs.Create(p, permission, overwrite, bufferSize, replication, blockSize, 
						progress, favoredNodes);
				}
				throw new NotSupportedException("Cannot create with" + " favoredNodes through a symlink to a non-DistributedFileSystem: "
					 + f + " -> " + p);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly Path f;

			private readonly FsPermission permission;

			private readonly bool overwrite;

			private readonly short replication;

			private readonly long blockSize;

			private readonly Progressable progress;

			private readonly int bufferSize;

			private readonly IPEndPoint[] favoredNodes;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Create(Path f, FsPermission permission, EnumSet
			<CreateFlag> cflags, int bufferSize, short replication, long blockSize, Progressable
			 progress, Options.ChecksumOpt checksumOpt)
		{
			statistics.IncrementWriteOps(1);
			Path absF = FixRelativePart(f);
			return new _FileSystemLinkResolver_444(this, permission, cflags, replication, blockSize
				, progress, bufferSize, checksumOpt).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_444 : FileSystemLinkResolver<FSDataOutputStream
			>
		{
			public _FileSystemLinkResolver_444(DistributedFileSystem _enclosing, FsPermission
				 permission, EnumSet<CreateFlag> cflags, short replication, long blockSize, Progressable
				 progress, int bufferSize, Options.ChecksumOpt checksumOpt)
			{
				this._enclosing = _enclosing;
				this.permission = permission;
				this.cflags = cflags;
				this.replication = replication;
				this.blockSize = blockSize;
				this.progress = progress;
				this.bufferSize = bufferSize;
				this.checksumOpt = checksumOpt;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override FSDataOutputStream DoCall(Path p)
			{
				DFSOutputStream dfsos = this._enclosing.dfs.Create(this._enclosing.GetPathName(p)
					, permission, cflags, replication, blockSize, progress, bufferSize, checksumOpt);
				return this._enclosing.dfs.CreateWrappedOutputStream(dfsos, this._enclosing.statistics
					);
			}

			/// <exception cref="System.IO.IOException"/>
			public override FSDataOutputStream Next(FileSystem fs, Path p)
			{
				return fs.Create(p, permission, cflags, bufferSize, replication, blockSize, progress
					, checksumOpt);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly FsPermission permission;

			private readonly EnumSet<CreateFlag> cflags;

			private readonly short replication;

			private readonly long blockSize;

			private readonly Progressable progress;

			private readonly int bufferSize;

			private readonly Options.ChecksumOpt checksumOpt;
		}

		/// <exception cref="System.IO.IOException"/>
		protected override FSDataOutputStream PrimitiveCreate(Path f, FsPermission absolutePermission
			, EnumSet<CreateFlag> flag, int bufferSize, short replication, long blockSize, Progressable
			 progress, Options.ChecksumOpt checksumOpt)
		{
			statistics.IncrementWriteOps(1);
			DFSOutputStream dfsos = dfs.PrimitiveCreate(GetPathName(FixRelativePart(f)), absolutePermission
				, flag, true, replication, blockSize, progress, bufferSize, checksumOpt);
			return dfs.CreateWrappedOutputStream(dfsos, statistics);
		}

		/// <summary>Same as create(), except fails if parent directory doesn't already exist.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream CreateNonRecursive(Path f, FsPermission permission
			, EnumSet<CreateFlag> flag, int bufferSize, short replication, long blockSize, Progressable
			 progress)
		{
			statistics.IncrementWriteOps(1);
			if (flag.Contains(CreateFlag.Overwrite))
			{
				flag.AddItem(CreateFlag.Create);
			}
			Path absF = FixRelativePart(f);
			return new _FileSystemLinkResolver_489(this, permission, flag, replication, blockSize
				, progress, bufferSize).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_489 : FileSystemLinkResolver<FSDataOutputStream
			>
		{
			public _FileSystemLinkResolver_489(DistributedFileSystem _enclosing, FsPermission
				 permission, EnumSet<CreateFlag> flag, short replication, long blockSize, Progressable
				 progress, int bufferSize)
			{
				this._enclosing = _enclosing;
				this.permission = permission;
				this.flag = flag;
				this.replication = replication;
				this.blockSize = blockSize;
				this.progress = progress;
				this.bufferSize = bufferSize;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override FSDataOutputStream DoCall(Path p)
			{
				DFSOutputStream dfsos = this._enclosing.dfs.Create(this._enclosing.GetPathName(p)
					, permission, flag, false, replication, blockSize, progress, bufferSize, null);
				return this._enclosing.dfs.CreateWrappedOutputStream(dfsos, this._enclosing.statistics
					);
			}

			/// <exception cref="System.IO.IOException"/>
			public override FSDataOutputStream Next(FileSystem fs, Path p)
			{
				return fs.CreateNonRecursive(p, permission, flag, bufferSize, replication, blockSize
					, progress);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly FsPermission permission;

			private readonly EnumSet<CreateFlag> flag;

			private readonly short replication;

			private readonly long blockSize;

			private readonly Progressable progress;

			private readonly int bufferSize;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool SetReplication(Path src, short replication)
		{
			statistics.IncrementWriteOps(1);
			Path absF = FixRelativePart(src);
			return new _FileSystemLinkResolver_513(this, replication).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_513 : FileSystemLinkResolver<bool>
		{
			public _FileSystemLinkResolver_513(DistributedFileSystem _enclosing, short replication
				)
			{
				this._enclosing = _enclosing;
				this.replication = replication;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override bool DoCall(Path p)
			{
				return this._enclosing.dfs.SetReplication(this._enclosing.GetPathName(p), replication
					);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Next(FileSystem fs, Path p)
			{
				return fs.SetReplication(p, replication);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly short replication;
		}

		/// <summary>Set the source path to the specified storage policy.</summary>
		/// <param name="src">The source path referring to either a directory or a file.</param>
		/// <param name="policyName">The name of the storage policy.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetStoragePolicy(Path src, string policyName)
		{
			statistics.IncrementWriteOps(1);
			Path absF = FixRelativePart(src);
			new _FileSystemLinkResolver_537(this, policyName, src).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_537 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_537(DistributedFileSystem _enclosing, string policyName
				, Path src)
			{
				this._enclosing = _enclosing;
				this.policyName = policyName;
				this.src = src;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.SetStoragePolicy(this._enclosing.GetPathName(p), policyName);
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				if (fs is Org.Apache.Hadoop.Hdfs.DistributedFileSystem)
				{
					((Org.Apache.Hadoop.Hdfs.DistributedFileSystem)fs).SetStoragePolicy(p, policyName
						);
					return null;
				}
				else
				{
					throw new NotSupportedException("Cannot perform setStoragePolicy on a non-DistributedFileSystem: "
						 + src + " -> " + p);
				}
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly string policyName;

			private readonly Path src;
		}

		/// <summary>Get all the existing storage policies</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual BlockStoragePolicy[] GetStoragePolicies()
		{
			statistics.IncrementReadOps(1);
			return dfs.GetStoragePolicies();
		}

		/// <summary>Move blocks from srcs to trg and delete srcs afterwards.</summary>
		/// <remarks>
		/// Move blocks from srcs to trg and delete srcs afterwards.
		/// The file block sizes must be the same.
		/// </remarks>
		/// <param name="trg">existing file to append to</param>
		/// <param name="psrcs">list of files (same block size, same replication)</param>
		/// <exception cref="System.IO.IOException"/>
		public override void Concat(Path trg, Path[] psrcs)
		{
			statistics.IncrementWriteOps(1);
			// Make target absolute
			Path absF = FixRelativePart(trg);
			// Make all srcs absolute
			Path[] srcs = new Path[psrcs.Length];
			for (int i = 0; i < psrcs.Length; i++)
			{
				srcs[i] = FixRelativePart(psrcs[i]);
			}
			// Try the concat without resolving any links
			string[] srcsStr = new string[psrcs.Length];
			try
			{
				for (int i_1 = 0; i_1 < psrcs.Length; i_1++)
				{
					srcsStr[i_1] = GetPathName(srcs[i_1]);
				}
				dfs.Concat(GetPathName(absF), srcsStr);
			}
			catch (UnresolvedLinkException)
			{
				// Exception could be from trg or any src.
				// Fully resolve trg and srcs. Fail if any of them are a symlink.
				FileStatus stat = GetFileLinkStatus(absF);
				if (stat.IsSymlink())
				{
					throw new IOException("Cannot concat with a symlink target: " + trg + " -> " + stat
						.GetPath());
				}
				absF = FixRelativePart(stat.GetPath());
				for (int i_1 = 0; i_1 < psrcs.Length; i_1++)
				{
					stat = GetFileLinkStatus(srcs[i_1]);
					if (stat.IsSymlink())
					{
						throw new IOException("Cannot concat with a symlink src: " + psrcs[i_1] + " -> " 
							+ stat.GetPath());
					}
					srcs[i_1] = FixRelativePart(stat.GetPath());
				}
				// Try concat again. Can still race with another symlink.
				for (int i_2 = 0; i_2 < psrcs.Length; i_2++)
				{
					srcsStr[i_2] = GetPathName(srcs[i_2]);
				}
				dfs.Concat(GetPathName(absF), srcsStr);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Rename(Path src, Path dst)
		{
			statistics.IncrementWriteOps(1);
			Path absSrc = FixRelativePart(src);
			Path absDst = FixRelativePart(dst);
			// Try the rename without resolving first
			try
			{
				return dfs.Rename(GetPathName(absSrc), GetPathName(absDst));
			}
			catch (UnresolvedLinkException)
			{
				// Fully resolve the source
				Path source = GetFileLinkStatus(absSrc).GetPath();
				// Keep trying to resolve the destination
				return new _FileSystemLinkResolver_631(this, source).Resolve(this, absDst);
			}
		}

		private sealed class _FileSystemLinkResolver_631 : FileSystemLinkResolver<bool>
		{
			public _FileSystemLinkResolver_631(DistributedFileSystem _enclosing, Path source)
			{
				this._enclosing = _enclosing;
				this.source = source;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override bool DoCall(Path p)
			{
				return this._enclosing.dfs.Rename(this._enclosing.GetPathName(source), this._enclosing
					.GetPathName(p));
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Next(FileSystem fs, Path p)
			{
				// Should just throw an error in FileSystem#checkPath
				return this.DoCall(p);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly Path source;
		}

		/// <summary>This rename operation is guaranteed to be atomic.</summary>
		/// <exception cref="System.IO.IOException"/>
		protected override void Rename(Path src, Path dst, params Options.Rename[] options
			)
		{
			statistics.IncrementWriteOps(1);
			Path absSrc = FixRelativePart(src);
			Path absDst = FixRelativePart(dst);
			// Try the rename without resolving first
			try
			{
				dfs.Rename(GetPathName(absSrc), GetPathName(absDst), options);
			}
			catch (UnresolvedLinkException)
			{
				// Fully resolve the source
				Path source = GetFileLinkStatus(absSrc).GetPath();
				// Keep trying to resolve the destination
				new _FileSystemLinkResolver_664(this, source, options).Resolve(this, absDst);
			}
		}

		private sealed class _FileSystemLinkResolver_664 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_664(DistributedFileSystem _enclosing, Path source, 
				Options.Rename[] options)
			{
				this._enclosing = _enclosing;
				this.source = source;
				this.options = options;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.Rename(this._enclosing.GetPathName(source), this._enclosing.GetPathName
					(p), options);
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				// Should just throw an error in FileSystem#checkPath
				return this.DoCall(p);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly Path source;

			private readonly Options.Rename[] options;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Truncate(Path f, long newLength)
		{
			statistics.IncrementWriteOps(1);
			Path absF = FixRelativePart(f);
			return new _FileSystemLinkResolver_685(this, newLength).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_685 : FileSystemLinkResolver<bool>
		{
			public _FileSystemLinkResolver_685(DistributedFileSystem _enclosing, long newLength
				)
			{
				this._enclosing = _enclosing;
				this.newLength = newLength;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override bool DoCall(Path p)
			{
				return this._enclosing.dfs.Truncate(this._enclosing.GetPathName(p), newLength);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Next(FileSystem fs, Path p)
			{
				return fs.Truncate(p, newLength);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly long newLength;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Delete(Path f, bool recursive)
		{
			statistics.IncrementWriteOps(1);
			Path absF = FixRelativePart(f);
			return new _FileSystemLinkResolver_703(this, recursive).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_703 : FileSystemLinkResolver<bool>
		{
			public _FileSystemLinkResolver_703(DistributedFileSystem _enclosing, bool recursive
				)
			{
				this._enclosing = _enclosing;
				this.recursive = recursive;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override bool DoCall(Path p)
			{
				return this._enclosing.dfs.Delete(this._enclosing.GetPathName(p), recursive);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Next(FileSystem fs, Path p)
			{
				return fs.Delete(p, recursive);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly bool recursive;
		}

		/// <exception cref="System.IO.IOException"/>
		public override ContentSummary GetContentSummary(Path f)
		{
			statistics.IncrementReadOps(1);
			Path absF = FixRelativePart(f);
			return new _FileSystemLinkResolver_721(this).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_721 : FileSystemLinkResolver<ContentSummary
			>
		{
			public _FileSystemLinkResolver_721(DistributedFileSystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override ContentSummary DoCall(Path p)
			{
				return this._enclosing.dfs.GetContentSummary(this._enclosing.GetPathName(p));
			}

			/// <exception cref="System.IO.IOException"/>
			public override ContentSummary Next(FileSystem fs, Path p)
			{
				return fs.GetContentSummary(p);
			}

			private readonly DistributedFileSystem _enclosing;
		}

		/// <summary>Set a directory's quotas</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetQuota(string, long, long, Org.Apache.Hadoop.FS.StorageType)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetQuota(Path src, long namespaceQuota, long storagespaceQuota
			)
		{
			Path absF = FixRelativePart(src);
			new _FileSystemLinkResolver_741(this, namespaceQuota, storagespaceQuota).Resolve(
				this, absF);
		}

		private sealed class _FileSystemLinkResolver_741 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_741(DistributedFileSystem _enclosing, long namespaceQuota
				, long storagespaceQuota)
			{
				this._enclosing = _enclosing;
				this.namespaceQuota = namespaceQuota;
				this.storagespaceQuota = storagespaceQuota;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.SetQuota(this._enclosing.GetPathName(p), namespaceQuota, storagespaceQuota
					);
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				// setQuota is not defined in FileSystem, so we only can resolve
				// within this DFS
				return this.DoCall(p);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly long namespaceQuota;

			private readonly long storagespaceQuota;
		}

		/// <summary>Set the per type storage quota of a directory.</summary>
		/// <param name="src">target directory whose quota is to be modified.</param>
		/// <param name="type">storage type of the specific storage type quota to be modified.
		/// 	</param>
		/// <param name="quota">
		/// value of the specific storage type quota to be modified.
		/// Maybe
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.HdfsConstants.QuotaReset"/>
		/// to clear quota by storage type.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetQuotaByStorageType(Path src, StorageType type, long quota)
		{
			Path absF = FixRelativePart(src);
			new _FileSystemLinkResolver_770(this, type, quota).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_770 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_770(DistributedFileSystem _enclosing, StorageType 
				type, long quota)
			{
				this._enclosing = _enclosing;
				this.type = type;
				this.quota = quota;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.SetQuotaByStorageType(this._enclosing.GetPathName(p), type, quota
					);
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				// setQuotaByStorageType is not defined in FileSystem, so we only can resolve
				// within this DFS
				return this.DoCall(p);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly StorageType type;

			private readonly long quota;
		}

		/// <exception cref="System.IO.IOException"/>
		private FileStatus[] ListStatusInternal(Path p)
		{
			string src = GetPathName(p);
			// fetch the first batch of entries in the directory
			DirectoryListing thisListing = dfs.ListPaths(src, HdfsFileStatus.EmptyName);
			if (thisListing == null)
			{
				// the directory does not exist
				throw new FileNotFoundException("File " + p + " does not exist.");
			}
			HdfsFileStatus[] partialListing = thisListing.GetPartialListing();
			if (!thisListing.HasMore())
			{
				// got all entries of the directory
				FileStatus[] stats = new FileStatus[partialListing.Length];
				for (int i = 0; i < partialListing.Length; i++)
				{
					stats[i] = partialListing[i].MakeQualified(GetUri(), p);
				}
				statistics.IncrementReadOps(1);
				return stats;
			}
			// The directory size is too big that it needs to fetch more
			// estimate the total number of entries in the directory
			int totalNumEntries = partialListing.Length + thisListing.GetRemainingEntries();
			AList<FileStatus> listing = new AList<FileStatus>(totalNumEntries);
			// add the first batch of entries to the array list
			foreach (HdfsFileStatus fileStatus in partialListing)
			{
				listing.AddItem(fileStatus.MakeQualified(GetUri(), p));
			}
			statistics.IncrementLargeReadOps(1);
			do
			{
				// now fetch more entries
				thisListing = dfs.ListPaths(src, thisListing.GetLastName());
				if (thisListing == null)
				{
					// the directory is deleted
					throw new FileNotFoundException("File " + p + " does not exist.");
				}
				partialListing = thisListing.GetPartialListing();
				foreach (HdfsFileStatus fileStatus_1 in partialListing)
				{
					listing.AddItem(fileStatus_1.MakeQualified(GetUri(), p));
				}
				statistics.IncrementLargeReadOps(1);
			}
			while (thisListing.HasMore());
			return Sharpen.Collections.ToArray(listing, new FileStatus[listing.Count]);
		}

		/// <summary>
		/// List all the entries of a directory
		/// Note that this operation is not atomic for a large directory.
		/// </summary>
		/// <remarks>
		/// List all the entries of a directory
		/// Note that this operation is not atomic for a large directory.
		/// The entries of a directory may be fetched from NameNode multiple times.
		/// It only guarantees that  each name occurs once if a directory
		/// undergoes changes between the calls.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] ListStatus(Path p)
		{
			Path absF = FixRelativePart(p);
			return new _FileSystemLinkResolver_849(this).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_849 : FileSystemLinkResolver<FileStatus
			[]>
		{
			public _FileSystemLinkResolver_849(DistributedFileSystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override FileStatus[] DoCall(Path p)
			{
				return this._enclosing.ListStatusInternal(p);
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileStatus[] Next(FileSystem fs, Path p)
			{
				return fs.ListStatus(p);
			}

			private readonly DistributedFileSystem _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		protected override RemoteIterator<LocatedFileStatus> ListLocatedStatus(Path p, PathFilter
			 filter)
		{
			Path absF = FixRelativePart(p);
			return new _FileSystemLinkResolver_868(filter).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_868 : FileSystemLinkResolver<RemoteIterator
			<LocatedFileStatus>>
		{
			public _FileSystemLinkResolver_868(PathFilter filter)
			{
				this.filter = filter;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override RemoteIterator<LocatedFileStatus> DoCall(Path p)
			{
				return new DistributedFileSystem.DirListingIterator<LocatedFileStatus>(this, p, filter
					, true);
			}

			/// <exception cref="System.IO.IOException"/>
			public override RemoteIterator<LocatedFileStatus> Next(FileSystem fs, Path p)
			{
				if (fs is Org.Apache.Hadoop.Hdfs.DistributedFileSystem)
				{
					return ((Org.Apache.Hadoop.Hdfs.DistributedFileSystem)fs).ListLocatedStatus(p, filter
						);
				}
				// symlink resolution for this methos does not work cross file systems
				// because it is a protected method.
				throw new IOException("Link resolution does not work with multiple " + "file systems for listLocatedStatus(): "
					 + p);
			}

			private readonly PathFilter filter;
		}

		/// <summary>
		/// Returns a remote iterator so that followup calls are made on demand
		/// while consuming the entries.
		/// </summary>
		/// <remarks>
		/// Returns a remote iterator so that followup calls are made on demand
		/// while consuming the entries. This reduces memory consumption during
		/// listing of a large directory.
		/// </remarks>
		/// <param name="p">target path</param>
		/// <returns>remote iterator</returns>
		/// <exception cref="System.IO.IOException"/>
		public override RemoteIterator<FileStatus> ListStatusIterator(Path p)
		{
			Path absF = FixRelativePart(p);
			return new _FileSystemLinkResolver_902().Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_902 : FileSystemLinkResolver<RemoteIterator
			<FileStatus>>
		{
			public _FileSystemLinkResolver_902()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override RemoteIterator<FileStatus> DoCall(Path p)
			{
				return new DistributedFileSystem.DirListingIterator<FileStatus>(this, p, false);
			}

			/// <exception cref="System.IO.IOException"/>
			public override RemoteIterator<FileStatus> Next(FileSystem fs, Path p)
			{
				return ((Org.Apache.Hadoop.Hdfs.DistributedFileSystem)fs).ListStatusIterator(p);
			}
		}

		/// <summary>
		/// This class defines an iterator that returns
		/// the file status of each file/subdirectory of a directory
		/// if needLocation, status contains block location if it is a file
		/// throws a RuntimeException with the error as its cause.
		/// </summary>
		/// <?/>
		private class DirListingIterator<T> : RemoteIterator<T>
			where T : FileStatus
		{
			private DirectoryListing thisListing;

			private int i;

			private Path p;

			private string src;

			private T curStat = null;

			private PathFilter filter;

			private bool needLocation;

			/// <exception cref="System.IO.IOException"/>
			private DirListingIterator(DistributedFileSystem _enclosing, Path p, PathFilter filter
				, bool needLocation)
			{
				this._enclosing = _enclosing;
				this.p = p;
				this.src = this._enclosing.GetPathName(p);
				this.filter = filter;
				this.needLocation = needLocation;
				// fetch the first batch of entries in the directory
				this.thisListing = this._enclosing.dfs.ListPaths(this.src, HdfsFileStatus.EmptyName
					, needLocation);
				this._enclosing.statistics.IncrementReadOps(1);
				if (this.thisListing == null)
				{
					// the directory does not exist
					throw new FileNotFoundException("File " + p + " does not exist.");
				}
				this.i = 0;
			}

			/// <exception cref="System.IO.IOException"/>
			private DirListingIterator(DistributedFileSystem _enclosing, Path p, bool needLocation
				)
				: this(p, null, needLocation)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool HasNext()
			{
				while (this.curStat == null && this.HasNextNoFilter())
				{
					T next;
					HdfsFileStatus fileStat = this.thisListing.GetPartialListing()[this.i++];
					if (this.needLocation)
					{
						next = (T)((HdfsLocatedFileStatus)fileStat).MakeQualifiedLocated(this._enclosing.
							GetUri(), this.p);
					}
					else
					{
						next = (T)fileStat.MakeQualified(this._enclosing.GetUri(), this.p);
					}
					// apply filter if not null
					if (this.filter == null || this.filter.Accept(next.GetPath()))
					{
						this.curStat = next;
					}
				}
				return this.curStat != null;
			}

			/// <summary>Check if there is a next item before applying the given filter</summary>
			/// <exception cref="System.IO.IOException"/>
			private bool HasNextNoFilter()
			{
				if (this.thisListing == null)
				{
					return false;
				}
				if (this.i >= this.thisListing.GetPartialListing().Length && this.thisListing.HasMore
					())
				{
					// current listing is exhausted & fetch a new listing
					this.thisListing = this._enclosing.dfs.ListPaths(this.src, this.thisListing.GetLastName
						(), this.needLocation);
					this._enclosing.statistics.IncrementReadOps(1);
					if (this.thisListing == null)
					{
						return false;
					}
					this.i = 0;
				}
				return (this.i < this.thisListing.GetPartialListing().Length);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual T Next()
			{
				if (this.HasNext())
				{
					T tmp = this.curStat;
					this.curStat = null;
					return tmp;
				}
				throw new NoSuchElementException("No more entry in " + this.p);
			}

			private readonly DistributedFileSystem _enclosing;
		}

		/// <summary>Create a directory, only when the parent directories exist.</summary>
		/// <remarks>
		/// Create a directory, only when the parent directories exist.
		/// See
		/// <see cref="Org.Apache.Hadoop.FS.Permission.FsPermission.ApplyUMask(Org.Apache.Hadoop.FS.Permission.FsPermission)
		/// 	"/>
		/// for details of how
		/// the permission is applied.
		/// </remarks>
		/// <param name="f">The path to create</param>
		/// <param name="permission">
		/// The permission.  See FsPermission#applyUMask for
		/// details about how this is used to calculate the
		/// effective permission.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Mkdir(Path f, FsPermission permission)
		{
			return MkdirsInternal(f, permission, false);
		}

		/// <summary>Create a directory and its parent directories.</summary>
		/// <remarks>
		/// Create a directory and its parent directories.
		/// See
		/// <see cref="Org.Apache.Hadoop.FS.Permission.FsPermission.ApplyUMask(Org.Apache.Hadoop.FS.Permission.FsPermission)
		/// 	"/>
		/// for details of how
		/// the permission is applied.
		/// </remarks>
		/// <param name="f">The path to create</param>
		/// <param name="permission">
		/// The permission.  See FsPermission#applyUMask for
		/// details about how this is used to calculate the
		/// effective permission.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public override bool Mkdirs(Path f, FsPermission permission)
		{
			return MkdirsInternal(f, permission, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private bool MkdirsInternal(Path f, FsPermission permission, bool createParent)
		{
			statistics.IncrementWriteOps(1);
			Path absF = FixRelativePart(f);
			return new _FileSystemLinkResolver_1043(this, permission, createParent).Resolve(this
				, absF);
		}

		private sealed class _FileSystemLinkResolver_1043 : FileSystemLinkResolver<bool>
		{
			public _FileSystemLinkResolver_1043(DistributedFileSystem _enclosing, FsPermission
				 permission, bool createParent)
			{
				this._enclosing = _enclosing;
				this.permission = permission;
				this.createParent = createParent;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override bool DoCall(Path p)
			{
				return this._enclosing.dfs.Mkdirs(this._enclosing.GetPathName(p), permission, createParent
					);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Next(FileSystem fs, Path p)
			{
				// FileSystem doesn't have a non-recursive mkdir() method
				// Best we can do is error out
				if (!createParent)
				{
					throw new IOException("FileSystem does not support non-recursive" + "mkdir");
				}
				return fs.Mkdirs(p, permission);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly FsPermission permission;

			private readonly bool createParent;
		}

		/// <exception cref="System.IO.IOException"/>
		protected override bool PrimitiveMkdir(Path f, FsPermission absolutePermission)
		{
			statistics.IncrementWriteOps(1);
			return dfs.PrimitiveMkdir(GetPathName(f), absolutePermission);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			try
			{
				dfs.CloseOutputStreams(false);
				base.Close();
			}
			finally
			{
				dfs.Close();
			}
		}

		public override string ToString()
		{
			return "DFS[" + dfs + "]";
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual DFSClient GetClient()
		{
			return dfs;
		}

		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.FS.FsStatus instead")]
		public class DiskStatus : FsStatus
		{
			public DiskStatus(FsStatus stats)
				: base(stats.GetCapacity(), stats.GetUsed(), stats.GetRemaining())
			{
			}

			public DiskStatus(long capacity, long dfsUsed, long remaining)
				: base(capacity, dfsUsed, remaining)
			{
			}

			public virtual long GetDfsUsed()
			{
				return base.GetUsed();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsStatus GetStatus(Path p)
		{
			statistics.IncrementReadOps(1);
			return dfs.GetDiskStatus();
		}

		/// <summary>
		/// Return the disk usage of the filesystem, including total capacity,
		/// used space, and remaining space
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.FS.FileSystem.GetStatus() instead"
			)]
		public virtual DistributedFileSystem.DiskStatus GetDiskStatus()
		{
			return new DistributedFileSystem.DiskStatus(dfs.GetDiskStatus());
		}

		/// <summary>
		/// Return the total raw capacity of the filesystem, disregarding
		/// replication.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.FS.FileSystem.GetStatus() instead"
			)]
		public virtual long GetRawCapacity()
		{
			return dfs.GetDiskStatus().GetCapacity();
		}

		/// <summary>
		/// Return the total raw used space in the filesystem, disregarding
		/// replication.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.FS.FileSystem.GetStatus() instead"
			)]
		public virtual long GetRawUsed()
		{
			return dfs.GetDiskStatus().GetUsed();
		}

		/// <summary>Returns count of blocks with no good replicas left.</summary>
		/// <remarks>
		/// Returns count of blocks with no good replicas left. Normally should be
		/// zero.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetMissingBlocksCount()
		{
			return dfs.GetMissingBlocksCount();
		}

		/// <summary>
		/// Returns count of blocks with replication factor 1 and have
		/// lost the only replica.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetMissingReplOneBlocksCount()
		{
			return dfs.GetMissingReplOneBlocksCount();
		}

		/// <summary>Returns count of blocks with one of more replica missing.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetUnderReplicatedBlocksCount()
		{
			return dfs.GetUnderReplicatedBlocksCount();
		}

		/// <summary>Returns count of blocks with at least one replica marked corrupt.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetCorruptBlocksCount()
		{
			return dfs.GetCorruptBlocksCount();
		}

		/// <exception cref="System.IO.IOException"/>
		public override RemoteIterator<Path> ListCorruptFileBlocks(Path path)
		{
			return new CorruptFileBlockIterator(dfs, path);
		}

		/// <returns>datanode statistics.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeInfo[] GetDataNodeStats()
		{
			return GetDataNodeStats(HdfsConstants.DatanodeReportType.All);
		}

		/// <returns>datanode statistics for the given type.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeInfo[] GetDataNodeStats(HdfsConstants.DatanodeReportType type
			)
		{
			return dfs.DatanodeReport(type);
		}

		/// <summary>Enter, leave or get safe mode.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetSafeMode(Org.Apache.Hadoop.Hdfs.Protocol.HdfsConstants.SafeModeAction, bool)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool SetSafeMode(HdfsConstants.SafeModeAction action)
		{
			return SetSafeMode(action, false);
		}

		/// <summary>Enter, leave or get safe mode.</summary>
		/// <param name="action">
		/// One of SafeModeAction.ENTER, SafeModeAction.LEAVE and
		/// SafeModeAction.GET
		/// </param>
		/// <param name="isChecked">
		/// If true check only for Active NNs status, else check first NN's
		/// status
		/// </param>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetSafeMode(Org.Apache.Hadoop.Hdfs.Protocol.HdfsConstants.SafeModeAction, bool)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool SetSafeMode(HdfsConstants.SafeModeAction action, bool isChecked
			)
		{
			return dfs.SetSafeMode(action, isChecked);
		}

		/// <summary>Save namespace image.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SaveNamespace()"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SaveNamespace()
		{
			dfs.SaveNamespace();
		}

		/// <summary>Rolls the edit log on the active NameNode.</summary>
		/// <remarks>
		/// Rolls the edit log on the active NameNode.
		/// Requires super-user privileges.
		/// </remarks>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.RollEdits()"/>
		/// <returns>the transaction ID of the newly created segment</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual long RollEdits()
		{
			return dfs.RollEdits();
		}

		/// <summary>enable/disable/check restoreFaileStorage</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.RestoreFailedStorage(string)
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool RestoreFailedStorage(string arg)
		{
			return dfs.RestoreFailedStorage(arg);
		}

		/// <summary>
		/// Refreshes the list of hosts and excluded hosts from the configured
		/// files.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshNodes()
		{
			dfs.RefreshNodes();
		}

		/// <summary>Finalize previously upgraded files system state.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void FinalizeUpgrade()
		{
			dfs.FinalizeUpgrade();
		}

		/// <summary>Rolling upgrade: start/finalize/query.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual RollingUpgradeInfo RollingUpgrade(HdfsConstants.RollingUpgradeAction
			 action)
		{
			return dfs.RollingUpgrade(action);
		}

		/*
		* Requests the namenode to dump data strcutures into specified
		* file.
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual void MetaSave(string pathname)
		{
			dfs.MetaSave(pathname);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsServerDefaults GetServerDefaults()
		{
			return dfs.GetServerDefaults();
		}

		/// <summary>Returns the stat information about the file.</summary>
		/// <exception cref="System.IO.FileNotFoundException">if the file does not exist.</exception>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileStatus(Path f)
		{
			statistics.IncrementReadOps(1);
			Path absF = FixRelativePart(f);
			return new _FileSystemLinkResolver_1301(this).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_1301 : FileSystemLinkResolver<FileStatus
			>
		{
			public _FileSystemLinkResolver_1301(DistributedFileSystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override FileStatus DoCall(Path p)
			{
				HdfsFileStatus fi = this._enclosing.dfs.GetFileInfo(this._enclosing.GetPathName(p
					));
				if (fi != null)
				{
					return fi.MakeQualified(this._enclosing.GetUri(), p);
				}
				else
				{
					throw new FileNotFoundException("File does not exist: " + p);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileStatus Next(FileSystem fs, Path p)
			{
				return fs.GetFileStatus(p);
			}

			private readonly DistributedFileSystem _enclosing;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void CreateSymlink(Path target, Path link, bool createParent)
		{
			if (!FileSystem.AreSymlinksEnabled())
			{
				throw new NotSupportedException("Symlinks not supported");
			}
			statistics.IncrementWriteOps(1);
			Path absF = FixRelativePart(link);
			new _FileSystemLinkResolver_1332(this, target, createParent).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_1332 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_1332(DistributedFileSystem _enclosing, Path target
				, bool createParent)
			{
				this._enclosing = _enclosing;
				this.target = target;
				this.createParent = createParent;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.CreateSymlink(target.ToString(), this._enclosing.GetPathName(
					p), createParent);
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				fs.CreateSymlink(target, p, createParent);
				return null;
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly Path target;

			private readonly bool createParent;
		}

		public override bool SupportsSymlinks()
		{
			return true;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileLinkStatus(Path f)
		{
			statistics.IncrementReadOps(1);
			Path absF = FixRelativePart(f);
			FileStatus status = new _FileSystemLinkResolver_1359(this).Resolve(this, absF);
			// Fully-qualify the symlink
			if (status.IsSymlink())
			{
				Path targetQual = FSLinkResolver.QualifySymlinkTarget(this.GetUri(), status.GetPath
					(), status.GetSymlink());
				status.SetSymlink(targetQual);
			}
			return status;
		}

		private sealed class _FileSystemLinkResolver_1359 : FileSystemLinkResolver<FileStatus
			>
		{
			public _FileSystemLinkResolver_1359(DistributedFileSystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override FileStatus DoCall(Path p)
			{
				HdfsFileStatus fi = this._enclosing.dfs.GetFileLinkInfo(this._enclosing.GetPathName
					(p));
				if (fi != null)
				{
					return fi.MakeQualified(this._enclosing.GetUri(), p);
				}
				else
				{
					throw new FileNotFoundException("File does not exist: " + p);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override FileStatus Next(FileSystem fs, Path p)
			{
				return fs.GetFileLinkStatus(p);
			}

			private readonly DistributedFileSystem _enclosing;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetLinkTarget(Path f)
		{
			statistics.IncrementReadOps(1);
			Path absF = FixRelativePart(f);
			return new _FileSystemLinkResolver_1390(this).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_1390 : FileSystemLinkResolver<Path>
		{
			public _FileSystemLinkResolver_1390(DistributedFileSystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Path DoCall(Path p)
			{
				HdfsFileStatus fi = this._enclosing.dfs.GetFileLinkInfo(this._enclosing.GetPathName
					(p));
				if (fi != null)
				{
					return fi.MakeQualified(this._enclosing.GetUri(), p).GetSymlink();
				}
				else
				{
					throw new FileNotFoundException("File does not exist: " + p);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Path Next(FileSystem fs, Path p)
			{
				return fs.GetLinkTarget(p);
			}

			private readonly DistributedFileSystem _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		protected override Path ResolveLink(Path f)
		{
			statistics.IncrementReadOps(1);
			string target = dfs.GetLinkTarget(GetPathName(FixRelativePart(f)));
			if (target == null)
			{
				throw new FileNotFoundException("File does not exist: " + f.ToString());
			}
			return new Path(target);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileChecksum GetFileChecksum(Path f)
		{
			statistics.IncrementReadOps(1);
			Path absF = FixRelativePart(f);
			return new _FileSystemLinkResolver_1423(this).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_1423 : FileSystemLinkResolver<FileChecksum
			>
		{
			public _FileSystemLinkResolver_1423(DistributedFileSystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override FileChecksum DoCall(Path p)
			{
				return this._enclosing.dfs.GetFileChecksum(this._enclosing.GetPathName(p), long.MaxValue
					);
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileChecksum Next(FileSystem fs, Path p)
			{
				return fs.GetFileChecksum(p);
			}

			private readonly DistributedFileSystem _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileChecksum GetFileChecksum(Path f, long length)
		{
			statistics.IncrementReadOps(1);
			Path absF = FixRelativePart(f);
			return new _FileSystemLinkResolver_1443(this, length).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_1443 : FileSystemLinkResolver<FileChecksum
			>
		{
			public _FileSystemLinkResolver_1443(DistributedFileSystem _enclosing, long length
				)
			{
				this._enclosing = _enclosing;
				this.length = length;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override FileChecksum DoCall(Path p)
			{
				return this._enclosing.dfs.GetFileChecksum(this._enclosing.GetPathName(p), length
					);
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileChecksum Next(FileSystem fs, Path p)
			{
				if (fs is DistributedFileSystem)
				{
					return ((DistributedFileSystem)fs).GetFileChecksum(p, length);
				}
				else
				{
					throw new UnsupportedFileSystemException("getFileChecksum(Path, long) is not supported by "
						 + fs.GetType().Name);
				}
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly long length;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetPermission(Path p, FsPermission permission)
		{
			statistics.IncrementWriteOps(1);
			Path absF = FixRelativePart(p);
			new _FileSystemLinkResolver_1469(this, permission).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_1469 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_1469(DistributedFileSystem _enclosing, FsPermission
				 permission)
			{
				this._enclosing = _enclosing;
				this.permission = permission;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.SetPermission(this._enclosing.GetPathName(p), permission);
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				fs.SetPermission(p, permission);
				return null;
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly FsPermission permission;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetOwner(Path p, string username, string groupname)
		{
			if (username == null && groupname == null)
			{
				throw new IOException("username == null && groupname == null");
			}
			statistics.IncrementWriteOps(1);
			Path absF = FixRelativePart(p);
			new _FileSystemLinkResolver_1494(this, username, groupname).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_1494 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_1494(DistributedFileSystem _enclosing, string username
				, string groupname)
			{
				this._enclosing = _enclosing;
				this.username = username;
				this.groupname = groupname;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.SetOwner(this._enclosing.GetPathName(p), username, groupname);
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				fs.SetOwner(p, username, groupname);
				return null;
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly string username;

			private readonly string groupname;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetTimes(Path p, long mtime, long atime)
		{
			statistics.IncrementWriteOps(1);
			Path absF = FixRelativePart(p);
			new _FileSystemLinkResolver_1516(this, mtime, atime).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_1516 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_1516(DistributedFileSystem _enclosing, long mtime, 
				long atime)
			{
				this._enclosing = _enclosing;
				this.mtime = mtime;
				this.atime = atime;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.SetTimes(this._enclosing.GetPathName(p), mtime, atime);
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				fs.SetTimes(p, mtime, atime);
				return null;
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly long mtime;

			private readonly long atime;
		}

		protected override int GetDefaultPort()
		{
			return NameNode.DefaultPort;
		}

		/// <exception cref="System.IO.IOException"/>
		public override Org.Apache.Hadoop.Security.Token.Token<object> GetDelegationToken
			(string renewer)
		{
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> result = dfs.GetDelegationToken
				(renewer == null ? null : new Text(renewer));
			return result;
		}

		/// <summary>
		/// Requests the namenode to tell all datanodes to use a new, non-persistent
		/// bandwidth value for dfs.balance.bandwidthPerSec.
		/// </summary>
		/// <remarks>
		/// Requests the namenode to tell all datanodes to use a new, non-persistent
		/// bandwidth value for dfs.balance.bandwidthPerSec.
		/// The bandwidth parameter is the max number of bytes per second of network
		/// bandwidth to be used by a datanode during balancing.
		/// </remarks>
		/// <param name="bandwidth">Balancer bandwidth in bytes per second for all datanodes.
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetBalancerBandwidth(long bandwidth)
		{
			dfs.SetBalancerBandwidth(bandwidth);
		}

		/// <summary>Get a canonical service name for this file system.</summary>
		/// <remarks>
		/// Get a canonical service name for this file system. If the URI is logical,
		/// the hostname part of the URI will be returned.
		/// </remarks>
		/// <returns>a service string that uniquely identifies this file system.</returns>
		public override string GetCanonicalServiceName()
		{
			return dfs.GetCanonicalServiceName();
		}

		protected override URI CanonicalizeUri(URI uri)
		{
			if (HAUtil.IsLogicalUri(GetConf(), uri))
			{
				// Don't try to DNS-resolve logical URIs, since the 'authority'
				// portion isn't a proper hostname
				return uri;
			}
			else
			{
				return NetUtils.GetCanonicalUri(uri, GetDefaultPort());
			}
		}

		/// <summary>Utility function that returns if the NameNode is in safemode or not.</summary>
		/// <remarks>
		/// Utility function that returns if the NameNode is in safemode or not. In HA
		/// mode, this API will return only ActiveNN's safemode status.
		/// </remarks>
		/// <returns>true if NameNode is in safemode, false otherwise.</returns>
		/// <exception cref="System.IO.IOException">when there is an issue communicating with the NameNode
		/// 	</exception>
		public virtual bool IsInSafeMode()
		{
			return SetSafeMode(HdfsConstants.SafeModeAction.SafemodeGet, true);
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Client.HdfsAdmin.AllowSnapshot(Org.Apache.Hadoop.FS.Path)
		/// 	"></seealso>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AllowSnapshot(Path path)
		{
			Path absF = FixRelativePart(path);
			new _FileSystemLinkResolver_1596(this, path).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_1596 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_1596(DistributedFileSystem _enclosing, Path path)
			{
				this._enclosing = _enclosing;
				this.path = path;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.AllowSnapshot(this._enclosing.GetPathName(p));
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				if (fs is DistributedFileSystem)
				{
					DistributedFileSystem myDfs = (DistributedFileSystem)fs;
					myDfs.AllowSnapshot(p);
				}
				else
				{
					throw new NotSupportedException("Cannot perform snapshot" + " operations on a symlink to a non-DistributedFileSystem: "
						 + path + " -> " + p);
				}
				return null;
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly Path path;
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Client.HdfsAdmin.DisallowSnapshot(Org.Apache.Hadoop.FS.Path)
		/// 	"></seealso>
		/// <exception cref="System.IO.IOException"/>
		public virtual void DisallowSnapshot(Path path)
		{
			Path absF = FixRelativePart(path);
			new _FileSystemLinkResolver_1623(this, path).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_1623 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_1623(DistributedFileSystem _enclosing, Path path)
			{
				this._enclosing = _enclosing;
				this.path = path;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.DisallowSnapshot(this._enclosing.GetPathName(p));
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				if (fs is DistributedFileSystem)
				{
					DistributedFileSystem myDfs = (DistributedFileSystem)fs;
					myDfs.DisallowSnapshot(p);
				}
				else
				{
					throw new NotSupportedException("Cannot perform snapshot" + " operations on a symlink to a non-DistributedFileSystem: "
						 + path + " -> " + p);
				}
				return null;
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly Path path;
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path CreateSnapshot(Path path, string snapshotName)
		{
			Path absF = FixRelativePart(path);
			return new _FileSystemLinkResolver_1651(this, snapshotName, path).Resolve(this, absF
				);
		}

		private sealed class _FileSystemLinkResolver_1651 : FileSystemLinkResolver<Path>
		{
			public _FileSystemLinkResolver_1651(DistributedFileSystem _enclosing, string snapshotName
				, Path path)
			{
				this._enclosing = _enclosing;
				this.snapshotName = snapshotName;
				this.path = path;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Path DoCall(Path p)
			{
				return new Path(this._enclosing.dfs.CreateSnapshot(this._enclosing.GetPathName(p)
					, snapshotName));
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path Next(FileSystem fs, Path p)
			{
				if (fs is DistributedFileSystem)
				{
					DistributedFileSystem myDfs = (DistributedFileSystem)fs;
					return myDfs.CreateSnapshot(p);
				}
				else
				{
					throw new NotSupportedException("Cannot perform snapshot" + " operations on a symlink to a non-DistributedFileSystem: "
						 + path + " -> " + p);
				}
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly string snapshotName;

			private readonly Path path;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RenameSnapshot(Path path, string snapshotOldName, string snapshotNewName
			)
		{
			Path absF = FixRelativePart(path);
			new _FileSystemLinkResolver_1677(this, snapshotOldName, snapshotNewName, path).Resolve
				(this, absF);
		}

		private sealed class _FileSystemLinkResolver_1677 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_1677(DistributedFileSystem _enclosing, string snapshotOldName
				, string snapshotNewName, Path path)
			{
				this._enclosing = _enclosing;
				this.snapshotOldName = snapshotOldName;
				this.snapshotNewName = snapshotNewName;
				this.path = path;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.RenameSnapshot(this._enclosing.GetPathName(p), snapshotOldName
					, snapshotNewName);
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				if (fs is DistributedFileSystem)
				{
					DistributedFileSystem myDfs = (DistributedFileSystem)fs;
					myDfs.RenameSnapshot(p, snapshotOldName, snapshotNewName);
				}
				else
				{
					throw new NotSupportedException("Cannot perform snapshot" + " operations on a symlink to a non-DistributedFileSystem: "
						 + path + " -> " + p);
				}
				return null;
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly string snapshotOldName;

			private readonly string snapshotNewName;

			private readonly Path path;
		}

		/// <returns>All the snapshottable directories</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual SnapshottableDirectoryStatus[] GetSnapshottableDirListing()
		{
			return dfs.GetSnapshottableDirListing();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DeleteSnapshot(Path snapshotDir, string snapshotName)
		{
			Path absF = FixRelativePart(snapshotDir);
			new _FileSystemLinkResolver_1714(this, snapshotName, snapshotDir).Resolve(this, absF
				);
		}

		private sealed class _FileSystemLinkResolver_1714 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_1714(DistributedFileSystem _enclosing, string snapshotName
				, Path snapshotDir)
			{
				this._enclosing = _enclosing;
				this.snapshotName = snapshotName;
				this.snapshotDir = snapshotDir;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.DeleteSnapshot(this._enclosing.GetPathName(p), snapshotName);
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				if (fs is DistributedFileSystem)
				{
					DistributedFileSystem myDfs = (DistributedFileSystem)fs;
					myDfs.DeleteSnapshot(p, snapshotName);
				}
				else
				{
					throw new NotSupportedException("Cannot perform snapshot" + " operations on a symlink to a non-DistributedFileSystem: "
						 + snapshotDir + " -> " + p);
				}
				return null;
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly string snapshotName;

			private readonly Path snapshotDir;
		}

		/// <summary>
		/// Get the difference between two snapshots, or between a snapshot and the
		/// current tree of a directory.
		/// </summary>
		/// <seealso cref="DFSClient.GetSnapshotDiffReport(string, string, string)"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual SnapshotDiffReport GetSnapshotDiffReport(Path snapshotDir, string 
			fromSnapshot, string toSnapshot)
		{
			Path absF = FixRelativePart(snapshotDir);
			return new _FileSystemLinkResolver_1747(this, fromSnapshot, toSnapshot, snapshotDir
				).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_1747 : FileSystemLinkResolver<SnapshotDiffReport
			>
		{
			public _FileSystemLinkResolver_1747(DistributedFileSystem _enclosing, string fromSnapshot
				, string toSnapshot, Path snapshotDir)
			{
				this._enclosing = _enclosing;
				this.fromSnapshot = fromSnapshot;
				this.toSnapshot = toSnapshot;
				this.snapshotDir = snapshotDir;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override SnapshotDiffReport DoCall(Path p)
			{
				return this._enclosing.dfs.GetSnapshotDiffReport(this._enclosing.GetPathName(p), 
					fromSnapshot, toSnapshot);
			}

			/// <exception cref="System.IO.IOException"/>
			public override SnapshotDiffReport Next(FileSystem fs, Path p)
			{
				if (fs is DistributedFileSystem)
				{
					DistributedFileSystem myDfs = (DistributedFileSystem)fs;
					myDfs.GetSnapshotDiffReport(p, fromSnapshot, toSnapshot);
				}
				else
				{
					throw new NotSupportedException("Cannot perform snapshot" + " operations on a symlink to a non-DistributedFileSystem: "
						 + snapshotDir + " -> " + p);
				}
				return null;
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly string fromSnapshot;

			private readonly string toSnapshot;

			private readonly Path snapshotDir;
		}

		/// <summary>Get the close status of a file</summary>
		/// <param name="src">The path to the file</param>
		/// <returns>return true if file is closed</returns>
		/// <exception cref="System.IO.FileNotFoundException">if the file does not exist.</exception>
		/// <exception cref="System.IO.IOException">If an I/O error occurred</exception>
		public virtual bool IsFileClosed(Path src)
		{
			Path absF = FixRelativePart(src);
			return new _FileSystemLinkResolver_1781(this, src).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_1781 : FileSystemLinkResolver<bool>
		{
			public _FileSystemLinkResolver_1781(DistributedFileSystem _enclosing, Path src)
			{
				this._enclosing = _enclosing;
				this.src = src;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override bool DoCall(Path p)
			{
				return this._enclosing.dfs.IsFileClosed(this._enclosing.GetPathName(p));
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Next(FileSystem fs, Path p)
			{
				if (fs is DistributedFileSystem)
				{
					DistributedFileSystem myDfs = (DistributedFileSystem)fs;
					return myDfs.IsFileClosed(p);
				}
				else
				{
					throw new NotSupportedException("Cannot call isFileClosed" + " on a symlink to a non-DistributedFileSystem: "
						 + src + " -> " + p);
				}
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly Path src;
		}

		/// <seealso>
		/// 
		/// <see cref="AddCacheDirective(Org.Apache.Hadoop.Hdfs.Protocol.CacheDirectiveInfo, Sharpen.EnumSet{E})
		/// 	"/>
		/// </seealso>
		/// <exception cref="System.IO.IOException"/>
		public virtual long AddCacheDirective(CacheDirectiveInfo info)
		{
			return AddCacheDirective(info, EnumSet.NoneOf<CacheFlag>());
		}

		/// <summary>Add a new CacheDirective.</summary>
		/// <param name="info">Information about a directive to add.</param>
		/// <param name="flags">
		/// 
		/// <see cref="Org.Apache.Hadoop.FS.CacheFlag"/>
		/// s to use for this operation.
		/// </param>
		/// <returns>the ID of the directive that was created.</returns>
		/// <exception cref="System.IO.IOException">if the directive could not be added</exception>
		public virtual long AddCacheDirective(CacheDirectiveInfo info, EnumSet<CacheFlag>
			 flags)
		{
			Preconditions.CheckNotNull(info.GetPath());
			Path path = new Path(GetPathName(FixRelativePart(info.GetPath()))).MakeQualified(
				GetUri(), GetWorkingDirectory());
			return dfs.AddCacheDirective(new CacheDirectiveInfo.Builder(info).SetPath(path).Build
				(), flags);
		}

		/// <seealso>
		/// 
		/// <see cref="ModifyCacheDirective(Org.Apache.Hadoop.Hdfs.Protocol.CacheDirectiveInfo, Sharpen.EnumSet{E})
		/// 	"/>
		/// </seealso>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ModifyCacheDirective(CacheDirectiveInfo info)
		{
			ModifyCacheDirective(info, EnumSet.NoneOf<CacheFlag>());
		}

		/// <summary>Modify a CacheDirective.</summary>
		/// <param name="info">
		/// Information about the directive to modify. You must set the ID
		/// to indicate which CacheDirective you want to modify.
		/// </param>
		/// <param name="flags">
		/// 
		/// <see cref="Org.Apache.Hadoop.FS.CacheFlag"/>
		/// s to use for this operation.
		/// </param>
		/// <exception cref="System.IO.IOException">if the directive could not be modified</exception>
		public virtual void ModifyCacheDirective(CacheDirectiveInfo info, EnumSet<CacheFlag
			> flags)
		{
			if (info.GetPath() != null)
			{
				info = new CacheDirectiveInfo.Builder(info).SetPath(new Path(GetPathName(FixRelativePart
					(info.GetPath()))).MakeQualified(GetUri(), GetWorkingDirectory())).Build();
			}
			dfs.ModifyCacheDirective(info, flags);
		}

		/// <summary>Remove a CacheDirectiveInfo.</summary>
		/// <param name="id">identifier of the CacheDirectiveInfo to remove</param>
		/// <exception cref="System.IO.IOException">if the directive could not be removed</exception>
		public virtual void RemoveCacheDirective(long id)
		{
			dfs.RemoveCacheDirective(id);
		}

		/// <summary>List cache directives.</summary>
		/// <remarks>List cache directives.  Incrementally fetches results from the server.</remarks>
		/// <param name="filter">
		/// Filter parameters to use when listing the directives, null to
		/// list all directives visible to us.
		/// </param>
		/// <returns>A RemoteIterator which returns CacheDirectiveInfo objects.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual RemoteIterator<CacheDirectiveEntry> ListCacheDirectives(CacheDirectiveInfo
			 filter)
		{
			if (filter == null)
			{
				filter = new CacheDirectiveInfo.Builder().Build();
			}
			if (filter.GetPath() != null)
			{
				filter = new CacheDirectiveInfo.Builder(filter).SetPath(new Path(GetPathName(FixRelativePart
					(filter.GetPath())))).Build();
			}
			RemoteIterator<CacheDirectiveEntry> iter = dfs.ListCacheDirectives(filter);
			return new _RemoteIterator_1885(this, iter);
		}

		private sealed class _RemoteIterator_1885 : RemoteIterator<CacheDirectiveEntry>
		{
			public _RemoteIterator_1885(DistributedFileSystem _enclosing, RemoteIterator<CacheDirectiveEntry
				> iter)
			{
				this._enclosing = _enclosing;
				this.iter = iter;
			}

			/// <exception cref="System.IO.IOException"/>
			public bool HasNext()
			{
				return iter.HasNext();
			}

			/// <exception cref="System.IO.IOException"/>
			public CacheDirectiveEntry Next()
			{
				// Although the paths we get back from the NameNode should always be
				// absolute, we call makeQualified to add the scheme and authority of
				// this DistributedFilesystem.
				CacheDirectiveEntry desc = iter.Next();
				CacheDirectiveInfo info = desc.GetInfo();
				Path p = info.GetPath().MakeQualified(this._enclosing.GetUri(), this._enclosing.GetWorkingDirectory
					());
				return new CacheDirectiveEntry(new CacheDirectiveInfo.Builder(info).SetPath(p).Build
					(), desc.GetStats());
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly RemoteIterator<CacheDirectiveEntry> iter;
		}

		/// <summary>Add a cache pool.</summary>
		/// <param name="info">The request to add a cache pool.</param>
		/// <exception cref="System.IO.IOException">
		/// 
		/// If the request could not be completed.
		/// </exception>
		public virtual void AddCachePool(CachePoolInfo info)
		{
			CachePoolInfo.Validate(info);
			dfs.AddCachePool(info);
		}

		/// <summary>Modify an existing cache pool.</summary>
		/// <param name="info">The request to modify a cache pool.</param>
		/// <exception cref="System.IO.IOException">
		/// 
		/// If the request could not be completed.
		/// </exception>
		public virtual void ModifyCachePool(CachePoolInfo info)
		{
			CachePoolInfo.Validate(info);
			dfs.ModifyCachePool(info);
		}

		/// <summary>Remove a cache pool.</summary>
		/// <param name="poolName">Name of the cache pool to remove.</param>
		/// <exception cref="System.IO.IOException">
		/// 
		/// if the cache pool did not exist, or could not be removed.
		/// </exception>
		public virtual void RemoveCachePool(string poolName)
		{
			CachePoolInfo.ValidateName(poolName);
			dfs.RemoveCachePool(poolName);
		}

		/// <summary>List all cache pools.</summary>
		/// <returns>
		/// A remote iterator from which you can get CachePoolEntry objects.
		/// Requests will be made as needed.
		/// </returns>
		/// <exception cref="System.IO.IOException">If there was an error listing cache pools.
		/// 	</exception>
		public virtual RemoteIterator<CachePoolEntry> ListCachePools()
		{
			return dfs.ListCachePools();
		}

		/// <summary><inheritDoc/></summary>
		/// <exception cref="System.IO.IOException"/>
		public override void ModifyAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			Path absF = FixRelativePart(path);
			new _FileSystemLinkResolver_1964(this, aclSpec).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_1964 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_1964(DistributedFileSystem _enclosing, IList<AclEntry
				> aclSpec)
			{
				this._enclosing = _enclosing;
				this.aclSpec = aclSpec;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.ModifyAclEntries(this._enclosing.GetPathName(p), aclSpec);
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				fs.ModifyAclEntries(p, aclSpec);
				return null;
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly IList<AclEntry> aclSpec;
		}

		/// <summary><inheritDoc/></summary>
		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			Path absF = FixRelativePart(path);
			new _FileSystemLinkResolver_1986(this, aclSpec).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_1986 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_1986(DistributedFileSystem _enclosing, IList<AclEntry
				> aclSpec)
			{
				this._enclosing = _enclosing;
				this.aclSpec = aclSpec;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.RemoveAclEntries(this._enclosing.GetPathName(p), aclSpec);
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				fs.RemoveAclEntries(p, aclSpec);
				return null;
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly IList<AclEntry> aclSpec;
		}

		/// <summary><inheritDoc/></summary>
		/// <exception cref="System.IO.IOException"/>
		public override void RemoveDefaultAcl(Path path)
		{
			Path absF = FixRelativePart(path);
			new _FileSystemLinkResolver_2007(this).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_2007 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_2007(DistributedFileSystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.RemoveDefaultAcl(this._enclosing.GetPathName(p));
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				fs.RemoveDefaultAcl(p);
				return null;
			}

			private readonly DistributedFileSystem _enclosing;
		}

		/// <summary><inheritDoc/></summary>
		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAcl(Path path)
		{
			Path absF = FixRelativePart(path);
			new _FileSystemLinkResolver_2028(this).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_2028 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_2028(DistributedFileSystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.RemoveAcl(this._enclosing.GetPathName(p));
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				fs.RemoveAcl(p);
				return null;
			}

			private readonly DistributedFileSystem _enclosing;
		}

		/// <summary><inheritDoc/></summary>
		/// <exception cref="System.IO.IOException"/>
		public override void SetAcl(Path path, IList<AclEntry> aclSpec)
		{
			Path absF = FixRelativePart(path);
			new _FileSystemLinkResolver_2049(this, aclSpec).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_2049 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_2049(DistributedFileSystem _enclosing, IList<AclEntry
				> aclSpec)
			{
				this._enclosing = _enclosing;
				this.aclSpec = aclSpec;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.SetAcl(this._enclosing.GetPathName(p), aclSpec);
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				fs.SetAcl(p, aclSpec);
				return null;
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly IList<AclEntry> aclSpec;
		}

		/// <summary><inheritDoc/></summary>
		/// <exception cref="System.IO.IOException"/>
		public override AclStatus GetAclStatus(Path path)
		{
			Path absF = FixRelativePart(path);
			return new _FileSystemLinkResolver_2070(this).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_2070 : FileSystemLinkResolver<AclStatus
			>
		{
			public _FileSystemLinkResolver_2070(DistributedFileSystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public override AclStatus DoCall(Path p)
			{
				return this._enclosing.dfs.GetAclStatus(this._enclosing.GetPathName(p));
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override AclStatus Next(FileSystem fs, Path p)
			{
				return fs.GetAclStatus(p);
			}

			private readonly DistributedFileSystem _enclosing;
		}

		/* HDFS only */
		/// <exception cref="System.IO.IOException"/>
		public virtual void CreateEncryptionZone(Path path, string keyName)
		{
			dfs.CreateEncryptionZone(GetPathName(path), keyName);
		}

		/* HDFS only */
		/// <exception cref="System.IO.IOException"/>
		public virtual EncryptionZone GetEZForPath(Path path)
		{
			Preconditions.CheckNotNull(path);
			return dfs.GetEZForPath(GetPathName(path));
		}

		/* HDFS only */
		/// <exception cref="System.IO.IOException"/>
		public virtual RemoteIterator<EncryptionZone> ListEncryptionZones()
		{
			return dfs.ListEncryptionZones();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetXAttr(Path path, string name, byte[] value, EnumSet<XAttrSetFlag
			> flag)
		{
			Path absF = FixRelativePart(path);
			new _FileSystemLinkResolver_2106(this, name, value, flag).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_2106 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_2106(DistributedFileSystem _enclosing, string name
				, byte[] value, EnumSet<XAttrSetFlag> flag)
			{
				this._enclosing = _enclosing;
				this.name = name;
				this.value = value;
				this.flag = flag;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.SetXAttr(this._enclosing.GetPathName(p), name, value, flag);
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				fs.SetXAttr(p, name, value, flag);
				return null;
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly string name;

			private readonly byte[] value;

			private readonly EnumSet<XAttrSetFlag> flag;
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] GetXAttr(Path path, string name)
		{
			Path absF = FixRelativePart(path);
			return new _FileSystemLinkResolver_2125(this, name).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_2125 : FileSystemLinkResolver<byte[]
			>
		{
			public _FileSystemLinkResolver_2125(DistributedFileSystem _enclosing, string name
				)
			{
				this._enclosing = _enclosing;
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			public override byte[] DoCall(Path p)
			{
				return this._enclosing.dfs.GetXAttr(this._enclosing.GetPathName(p), name);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override byte[] Next(FileSystem fs, Path p)
			{
				return fs.GetXAttr(p, name);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly string name;
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path path)
		{
			Path absF = FixRelativePart(path);
			return new _FileSystemLinkResolver_2141(this).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_2141 : FileSystemLinkResolver<IDictionary
			<string, byte[]>>
		{
			public _FileSystemLinkResolver_2141(DistributedFileSystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public override IDictionary<string, byte[]> DoCall(Path p)
			{
				return this._enclosing.dfs.GetXAttrs(this._enclosing.GetPathName(p));
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override IDictionary<string, byte[]> Next(FileSystem fs, Path p)
			{
				return fs.GetXAttrs(p);
			}

			private readonly DistributedFileSystem _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path path, IList<string> names
			)
		{
			Path absF = FixRelativePart(path);
			return new _FileSystemLinkResolver_2158(this, names).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_2158 : FileSystemLinkResolver<IDictionary
			<string, byte[]>>
		{
			public _FileSystemLinkResolver_2158(DistributedFileSystem _enclosing, IList<string
				> names)
			{
				this._enclosing = _enclosing;
				this.names = names;
			}

			/// <exception cref="System.IO.IOException"/>
			public override IDictionary<string, byte[]> DoCall(Path p)
			{
				return this._enclosing.dfs.GetXAttrs(this._enclosing.GetPathName(p), names);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override IDictionary<string, byte[]> Next(FileSystem fs, Path p)
			{
				return fs.GetXAttrs(p, names);
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly IList<string> names;
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> ListXAttrs(Path path)
		{
			Path absF = FixRelativePart(path);
			return new _FileSystemLinkResolver_2175(this).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_2175 : FileSystemLinkResolver<IList<
			string>>
		{
			public _FileSystemLinkResolver_2175(DistributedFileSystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public override IList<string> DoCall(Path p)
			{
				return this._enclosing.dfs.ListXAttrs(this._enclosing.GetPathName(p));
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override IList<string> Next(FileSystem fs, Path p)
			{
				return fs.ListXAttrs(p);
			}

			private readonly DistributedFileSystem _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveXAttr(Path path, string name)
		{
			Path absF = FixRelativePart(path);
			new _FileSystemLinkResolver_2191(this, name).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_2191 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_2191(DistributedFileSystem _enclosing, string name
				)
			{
				this._enclosing = _enclosing;
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.RemoveXAttr(this._enclosing.GetPathName(p), name);
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				fs.RemoveXAttr(p, name);
				return null;
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly string name;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Access(Path path, FsAction mode)
		{
			Path absF = FixRelativePart(path);
			new _FileSystemLinkResolver_2209(this, mode).Resolve(this, absF);
		}

		private sealed class _FileSystemLinkResolver_2209 : FileSystemLinkResolver<Void>
		{
			public _FileSystemLinkResolver_2209(DistributedFileSystem _enclosing, FsAction mode
				)
			{
				this._enclosing = _enclosing;
				this.mode = mode;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void DoCall(Path p)
			{
				this._enclosing.dfs.CheckAccess(this._enclosing.GetPathName(p), mode);
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Void Next(FileSystem fs, Path p)
			{
				fs.Access(p, mode);
				return null;
			}

			private readonly DistributedFileSystem _enclosing;

			private readonly FsAction mode;
		}

		/// <exception cref="System.IO.IOException"/>
		public override Org.Apache.Hadoop.Security.Token.Token<object>[] AddDelegationTokens
			(string renewer, Credentials credentials)
		{
			Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = base.AddDelegationTokens
				(renewer, credentials);
			if (dfs.IsHDFSEncryptionEnabled())
			{
				KeyProviderDelegationTokenExtension keyProviderDelegationTokenExtension = KeyProviderDelegationTokenExtension
					.CreateKeyProviderDelegationTokenExtension(dfs.GetKeyProvider());
				Org.Apache.Hadoop.Security.Token.Token<object>[] kpTokens = keyProviderDelegationTokenExtension
					.AddDelegationTokens(renewer, credentials);
				if (tokens != null && kpTokens != null)
				{
					Org.Apache.Hadoop.Security.Token.Token<object>[] all = new Org.Apache.Hadoop.Security.Token.Token
						<object>[tokens.Length + kpTokens.Length];
					System.Array.Copy(tokens, 0, all, 0, tokens.Length);
					System.Array.Copy(kpTokens, 0, all, tokens.Length, kpTokens.Length);
					tokens = all;
				}
				else
				{
					tokens = (tokens != null) ? tokens : kpTokens;
				}
			}
			return tokens;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DFSInotifyEventInputStream GetInotifyEventStream()
		{
			return dfs.GetInotifyEventStream();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DFSInotifyEventInputStream GetInotifyEventStream(long lastReadTxid
			)
		{
			return dfs.GetInotifyEventStream(lastReadTxid);
		}
	}
}
