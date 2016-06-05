using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class Hdfs : AbstractFileSystem
	{
		internal DFSClient dfs;

		private bool verifyChecksum = true;

		static Hdfs()
		{
			HdfsConfiguration.Init();
		}

		/// <summary>
		/// This constructor has the signature needed by
		/// <see cref="AbstractFileSystem.CreateFileSystem(Sharpen.URI, Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// </summary>
		/// <param name="theUri">which must be that of Hdfs</param>
		/// <param name="conf">configuration</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		internal Hdfs(URI theUri, Configuration conf)
			: base(theUri, HdfsConstants.HdfsUriScheme, true, NameNode.DefaultPort)
		{
			if (!Sharpen.Runtime.EqualsIgnoreCase(theUri.GetScheme(), HdfsConstants.HdfsUriScheme
				))
			{
				throw new ArgumentException("Passed URI's scheme is not for Hdfs");
			}
			string host = theUri.GetHost();
			if (host == null)
			{
				throw new IOException("Incomplete HDFS URI, no host: " + theUri);
			}
			this.dfs = new DFSClient(theUri, conf, GetStatistics());
		}

		public override int GetUriDefaultPort()
		{
			return NameNode.DefaultPort;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream CreateInternal(Path f, EnumSet<CreateFlag> createFlag
			, FsPermission absolutePermission, int bufferSize, short replication, long blockSize
			, Progressable progress, Options.ChecksumOpt checksumOpt, bool createParent)
		{
			DFSOutputStream dfsos = dfs.PrimitiveCreate(GetUriPath(f), absolutePermission, createFlag
				, createParent, replication, blockSize, progress, bufferSize, checksumOpt);
			return dfs.CreateWrappedOutputStream(dfsos, statistics, dfsos.GetInitialLen());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override bool Delete(Path f, bool recursive)
		{
			return dfs.Delete(GetUriPath(f), recursive);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override BlockLocation[] GetFileBlockLocations(Path p, long start, long len
			)
		{
			return dfs.GetBlockLocations(GetUriPath(p), start, len);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FileChecksum GetFileChecksum(Path f)
		{
			return dfs.GetFileChecksum(GetUriPath(f), long.MaxValue);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FileStatus GetFileStatus(Path f)
		{
			HdfsFileStatus fi = dfs.GetFileInfo(GetUriPath(f));
			if (fi != null)
			{
				return fi.MakeQualified(GetUri(), f);
			}
			else
			{
				throw new FileNotFoundException("File does not exist: " + f.ToString());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FileStatus GetFileLinkStatus(Path f)
		{
			HdfsFileStatus fi = dfs.GetFileLinkInfo(GetUriPath(f));
			if (fi != null)
			{
				return fi.MakeQualified(GetUri(), f);
			}
			else
			{
				throw new FileNotFoundException("File does not exist: " + f);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsStatus GetFsStatus()
		{
			return dfs.GetDiskStatus();
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsServerDefaults GetServerDefaults()
		{
			return dfs.GetServerDefaults();
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override RemoteIterator<LocatedFileStatus> ListLocatedStatus(Path p)
		{
			return new _DirListingIterator_163(this, p, p, true);
		}

		private sealed class _DirListingIterator_163 : Hdfs.DirListingIterator<LocatedFileStatus
			>
		{
			public _DirListingIterator_163(Hdfs _enclosing, Path p, Path baseArg1, bool baseArg2
				)
				: base(_enclosing, baseArg1, baseArg2)
			{
				this._enclosing = _enclosing;
				this.p = p;
			}

			/// <exception cref="System.IO.IOException"/>
			public override LocatedFileStatus Next()
			{
				return ((HdfsLocatedFileStatus)this.GetNext()).MakeQualifiedLocated(this._enclosing
					.GetUri(), p);
			}

			private readonly Hdfs _enclosing;

			private readonly Path p;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public override RemoteIterator<FileStatus> ListStatusIterator(Path f)
		{
			return new _DirListingIterator_177(this, f, f, false);
		}

		private sealed class _DirListingIterator_177 : Hdfs.DirListingIterator<FileStatus
			>
		{
			public _DirListingIterator_177(Hdfs _enclosing, Path f, Path baseArg1, bool baseArg2
				)
				: base(_enclosing, baseArg1, baseArg2)
			{
				this._enclosing = _enclosing;
				this.f = f;
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileStatus Next()
			{
				return this.GetNext().MakeQualified(this._enclosing.GetUri(), f);
			}

			private readonly Hdfs _enclosing;

			private readonly Path f;
		}

		/// <summary>
		/// This class defines an iterator that returns
		/// the file status of each file/subdirectory of a directory
		/// if needLocation, status contains block location if it is a file
		/// throws a RuntimeException with the error as its cause.
		/// </summary>
		/// <?/>
		private abstract class DirListingIterator<T> : RemoteIterator<T>
			where T : FileStatus
		{
			private DirectoryListing thisListing;

			private int i;

			private readonly string src;

			private readonly bool needLocation;

			/// <exception cref="System.IO.IOException"/>
			private DirListingIterator(Hdfs _enclosing, Path p, bool needLocation)
			{
				this._enclosing = _enclosing;
				// if status
				this.src = this._enclosing.GetUriPath(p);
				this.needLocation = needLocation;
				// fetch the first batch of entries in the directory
				this.thisListing = this._enclosing.dfs.ListPaths(this.src, HdfsFileStatus.EmptyName
					, needLocation);
				if (this.thisListing == null)
				{
					// the directory does not exist
					throw new FileNotFoundException("File " + this.src + " does not exist.");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool HasNext()
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
					if (this.thisListing == null)
					{
						return false;
					}
					// the directory is deleted
					this.i = 0;
				}
				return (this.i < this.thisListing.GetPartialListing().Length);
			}

			/// <summary>Get the next item in the list</summary>
			/// <returns>the next item in the list</returns>
			/// <exception cref="System.IO.IOException">if there is any error</exception>
			/// <exception cref="NoSuchElmentException">if no more entry is available</exception>
			public virtual HdfsFileStatus GetNext()
			{
				if (this.HasNext())
				{
					return this.thisListing.GetPartialListing()[this.i++];
				}
				throw new NoSuchElementException("No more entry in " + this.src);
			}

			public abstract T Next();

			private readonly Hdfs _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			string src = GetUriPath(f);
			// fetch the first batch of entries in the directory
			DirectoryListing thisListing = dfs.ListPaths(src, HdfsFileStatus.EmptyName);
			if (thisListing == null)
			{
				// the directory does not exist
				throw new FileNotFoundException("File " + f + " does not exist.");
			}
			HdfsFileStatus[] partialListing = thisListing.GetPartialListing();
			if (!thisListing.HasMore())
			{
				// got all entries of the directory
				FileStatus[] stats = new FileStatus[partialListing.Length];
				for (int i = 0; i < partialListing.Length; i++)
				{
					stats[i] = partialListing[i].MakeQualified(GetUri(), f);
				}
				return stats;
			}
			// The directory size is too big that it needs to fetch more
			// estimate the total number of entries in the directory
			int totalNumEntries = partialListing.Length + thisListing.GetRemainingEntries();
			AList<FileStatus> listing = new AList<FileStatus>(totalNumEntries);
			// add the first batch of entries to the array list
			foreach (HdfsFileStatus fileStatus in partialListing)
			{
				listing.AddItem(fileStatus.MakeQualified(GetUri(), f));
			}
			do
			{
				// now fetch more entries
				thisListing = dfs.ListPaths(src, thisListing.GetLastName());
				if (thisListing == null)
				{
					// the directory is deleted
					throw new FileNotFoundException("File " + f + " does not exist.");
				}
				partialListing = thisListing.GetPartialListing();
				foreach (HdfsFileStatus fileStatus_1 in partialListing)
				{
					listing.AddItem(fileStatus_1.MakeQualified(GetUri(), f));
				}
			}
			while (thisListing.HasMore());
			return Sharpen.Collections.ToArray(listing, new FileStatus[listing.Count]);
		}

		/// <exception cref="System.IO.IOException"/>
		public override RemoteIterator<Path> ListCorruptFileBlocks(Path path)
		{
			return new CorruptFileBlockIterator(dfs, path);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void Mkdir(Path dir, FsPermission permission, bool createParent)
		{
			dfs.PrimitiveMkdir(GetUriPath(dir), permission, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FSDataInputStream Open(Path f, int bufferSize)
		{
			DFSInputStream dfsis = dfs.Open(GetUriPath(f), bufferSize, verifyChecksum);
			return dfs.CreateWrappedInputStream(dfsis);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override bool Truncate(Path f, long newLength)
		{
			return dfs.Truncate(GetUriPath(f), newLength);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void RenameInternal(Path src, Path dst)
		{
			dfs.Rename(GetUriPath(src), GetUriPath(dst), Options.Rename.None);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void RenameInternal(Path src, Path dst, bool overwrite)
		{
			dfs.Rename(GetUriPath(src), GetUriPath(dst), overwrite ? Options.Rename.Overwrite
				 : Options.Rename.None);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void SetOwner(Path f, string username, string groupname)
		{
			dfs.SetOwner(GetUriPath(f), username, groupname);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void SetPermission(Path f, FsPermission permission)
		{
			dfs.SetPermission(GetUriPath(f), permission);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override bool SetReplication(Path f, short replication)
		{
			return dfs.SetReplication(GetUriPath(f), replication);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void SetTimes(Path f, long mtime, long atime)
		{
			dfs.SetTimes(GetUriPath(f), mtime, atime);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetVerifyChecksum(bool verifyChecksum)
		{
			this.verifyChecksum = verifyChecksum;
		}

		public override bool SupportsSymlinks()
		{
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void CreateSymlink(Path target, Path link, bool createParent)
		{
			dfs.CreateSymlink(target.ToString(), GetUriPath(link), createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path GetLinkTarget(Path p)
		{
			return new Path(dfs.GetLinkTarget(GetUriPath(p)));
		}

		public override string GetCanonicalServiceName()
		{
			return dfs.GetCanonicalServiceName();
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<Org.Apache.Hadoop.Security.Token.Token<object>> GetDelegationTokens
			(string renewer)
		{
			//AbstractFileSystem
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> result = dfs.GetDelegationToken
				(renewer == null ? null : new Text(renewer));
			IList<Org.Apache.Hadoop.Security.Token.Token<object>> tokenList = new AList<Org.Apache.Hadoop.Security.Token.Token
				<object>>();
			tokenList.AddItem(result);
			return tokenList;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ModifyAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			dfs.ModifyAclEntries(GetUriPath(path), aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			dfs.RemoveAclEntries(GetUriPath(path), aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveDefaultAcl(Path path)
		{
			dfs.RemoveDefaultAcl(GetUriPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAcl(Path path)
		{
			dfs.RemoveAcl(GetUriPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetAcl(Path path, IList<AclEntry> aclSpec)
		{
			dfs.SetAcl(GetUriPath(path), aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override AclStatus GetAclStatus(Path path)
		{
			return dfs.GetAclStatus(GetUriPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetXAttr(Path path, string name, byte[] value, EnumSet<XAttrSetFlag
			> flag)
		{
			dfs.SetXAttr(GetUriPath(path), name, value, flag);
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] GetXAttr(Path path, string name)
		{
			return dfs.GetXAttr(GetUriPath(path), name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path path)
		{
			return dfs.GetXAttrs(GetUriPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path path, IList<string> names
			)
		{
			return dfs.GetXAttrs(GetUriPath(path), names);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> ListXAttrs(Path path)
		{
			return dfs.ListXAttrs(GetUriPath(path));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveXAttr(Path path, string name)
		{
			dfs.RemoveXAttr(GetUriPath(path), name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Access(Path path, FsAction mode)
		{
			dfs.CheckAccess(GetUriPath(path), mode);
		}

		/// <summary>Renew an existing delegation token.</summary>
		/// <param name="token">delegation token obtained earlier</param>
		/// <returns>the new expiration time</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Token.renew instead.")]
		public virtual long RenewDelegationToken<_T0>(Org.Apache.Hadoop.Security.Token.Token
			<_T0> token)
			where _T0 : AbstractDelegationTokenIdentifier
		{
			return dfs.RenewDelegationToken((Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				>)token);
		}

		/// <summary>Cancel an existing delegation token.</summary>
		/// <param name="token">delegation token</param>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Token.cancel instead.")]
		public virtual void CancelDelegationToken<_T0>(Org.Apache.Hadoop.Security.Token.Token
			<_T0> token)
			where _T0 : AbstractDelegationTokenIdentifier
		{
			dfs.CancelDelegationToken((Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				>)token);
		}
	}
}
