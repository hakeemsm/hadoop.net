using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Options = Org.Apache.Hadoop.FS.Options;

namespace Hadoop.Common.Core.Fs
{
	/// <summary>
	/// A <code>FilterFileSystem</code> contains
	/// some other file system, which it uses as
	/// its  basic file system, possibly transforming
	/// the data along the way or providing  additional
	/// functionality.
	/// </summary>
	/// <remarks>
	/// A <code>FilterFileSystem</code> contains
	/// some other file system, which it uses as
	/// its  basic file system, possibly transforming
	/// the data along the way or providing  additional
	/// functionality. The class <code>FilterFileSystem</code>
	/// itself simply overrides all  methods of
	/// <code>FileSystem</code> with versions that
	/// pass all requests to the contained  file
	/// system. Subclasses of <code>FilterFileSystem</code>
	/// may further override some of  these methods
	/// and may also provide additional methods
	/// and fields.
	/// </remarks>
	public class FilterFileSystem : FileSystem
	{
		protected internal FileSystem fs;

		protected internal string swapScheme;

		public FilterFileSystem()
		{
		}

		public FilterFileSystem(FileSystem fs)
		{
			/*
			* so that extending classes can define it
			*/
			this.fs = fs;
			this.statistics = fs.statistics;
		}

		/// <summary>Get the raw file system</summary>
		/// <returns>FileSystem being filtered</returns>
		public virtual FileSystem GetRawFileSystem()
		{
			return fs;
		}

		/// <summary>Called after a new FileSystem instance is constructed.</summary>
		/// <param name="name">
		/// a uri whose authority section names the host, port, etc.
		/// for this FileSystem
		/// </param>
		/// <param name="conf">the configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public override void Initialize(Uri name, Configuration conf)
		{
			base.Initialize(name, conf);
			// this is less than ideal, but existing filesystems sometimes neglect
			// to initialize the embedded filesystem
			if (fs.GetConf() == null)
			{
				fs.Initialize(name, conf);
			}
			string scheme = name.GetScheme();
			if (!scheme.Equals(fs.GetUri().GetScheme()))
			{
				swapScheme = scheme;
			}
		}

		/// <summary>Returns a URI whose scheme and authority identify this FileSystem.</summary>
		public override URI GetUri()
		{
			return fs.GetUri();
		}

		protected internal override URI GetCanonicalUri()
		{
			return fs.GetCanonicalUri();
		}

		protected internal override URI CanonicalizeUri(URI uri)
		{
			return fs.CanonicalizeUri(uri);
		}

		/// <summary>Make sure that a path specifies a FileSystem.</summary>
		public override Path MakeQualified(Path path)
		{
			Path fqPath = fs.MakeQualified(path);
			// swap in our scheme if the filtered fs is using a different scheme
			if (swapScheme != null)
			{
				try
				{
					// NOTE: should deal with authority, but too much other stuff is broken 
					fqPath = new Path(new URI(swapScheme, fqPath.ToUri().GetSchemeSpecificPart(), null
						));
				}
				catch (URISyntaxException e)
				{
					throw new ArgumentException(e);
				}
			}
			return fqPath;
		}

		///////////////////////////////////////////////////////////////
		// FileSystem
		///////////////////////////////////////////////////////////////
		/// <summary>Check that a Path belongs to this FileSystem.</summary>
		protected internal override void CheckPath(Path path)
		{
			fs.CheckPath(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override BlockLocation[] GetFileBlockLocations(FileStatus file, long start
			, long len)
		{
			return fs.GetFileBlockLocations(file, start, len);
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path ResolvePath(Path p)
		{
			return fs.ResolvePath(p);
		}

		/// <summary>Opens an FSDataInputStream at the indicated Path.</summary>
		/// <param name="f">the file name to open</param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <exception cref="System.IO.IOException"/>
		public override FSDataInputStream Open(Path f, int bufferSize)
		{
			return fs.Open(f, bufferSize);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Append(Path f, int bufferSize, Progressable progress
			)
		{
			return fs.Append(f, bufferSize, progress);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Concat(Path f, Path[] psrcs)
		{
			fs.Concat(f, psrcs);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
			, int bufferSize, short replication, long blockSize, Progressable progress)
		{
			return fs.Create(f, permission, overwrite, bufferSize, replication, blockSize, progress
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Create(Path f, FsPermission permission, EnumSet
			<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable
			 progress, Options.ChecksumOpt checksumOpt)
		{
			return fs.Create(f, permission, flags, bufferSize, replication, blockSize, progress
				, checksumOpt);
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public override FSDataOutputStream CreateNonRecursive(Path f, FsPermission permission
			, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, 
			Progressable progress)
		{
			return fs.CreateNonRecursive(f, permission, flags, bufferSize, replication, blockSize
				, progress);
		}

		/// <summary>Set replication for an existing file.</summary>
		/// <param name="src">file name</param>
		/// <param name="replication">new replication</param>
		/// <exception cref="System.IO.IOException"/>
		/// <returns>
		/// true if successful;
		/// false if file does not exist or is a directory
		/// </returns>
		public override bool SetReplication(Path src, short replication)
		{
			return fs.SetReplication(src, replication);
		}

		/// <summary>Renames Path src to Path dst.</summary>
		/// <remarks>
		/// Renames Path src to Path dst.  Can take place on local fs
		/// or remote DFS.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override bool Rename(Path src, Path dst)
		{
			return fs.Rename(src, dst);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Truncate(Path f, long newLength)
		{
			return fs.Truncate(f, newLength);
		}

		/// <summary>Delete a file</summary>
		/// <exception cref="System.IO.IOException"/>
		public override bool Delete(Path f, bool recursive)
		{
			return fs.Delete(f, recursive);
		}

		/// <summary>List files in a directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			return fs.ListStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override RemoteIterator<Path> ListCorruptFileBlocks(Path path)
		{
			return fs.ListCorruptFileBlocks(path);
		}

		/// <summary>List files and its block locations in a directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override RemoteIterator<LocatedFileStatus> ListLocatedStatus(Path f)
		{
			return fs.ListLocatedStatus(f);
		}

		/// <summary>Return a remote iterator for listing in a directory</summary>
		/// <exception cref="System.IO.IOException"/>
		public override RemoteIterator<FileStatus> ListStatusIterator(Path f)
		{
			return fs.ListStatusIterator(f);
		}

		public override Path GetHomeDirectory()
		{
			return fs.GetHomeDirectory();
		}

		/// <summary>Set the current working directory for the given file system.</summary>
		/// <remarks>
		/// Set the current working directory for the given file system. All relative
		/// paths will be resolved relative to it.
		/// </remarks>
		/// <param name="newDir"/>
		public override void SetWorkingDirectory(Path newDir)
		{
			fs.SetWorkingDirectory(newDir);
		}

		/// <summary>Get the current working directory for the given file system</summary>
		/// <returns>the directory pathname</returns>
		public override Path GetWorkingDirectory()
		{
			return fs.GetWorkingDirectory();
		}

		protected internal override Path GetInitialWorkingDirectory()
		{
			return fs.GetInitialWorkingDirectory();
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsStatus GetStatus(Path p)
		{
			return fs.GetStatus(p);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Mkdirs(Path f, FsPermission permission)
		{
			return fs.Mkdirs(f, permission);
		}

		/// <summary>The src file is on the local disk.</summary>
		/// <remarks>
		/// The src file is on the local disk.  Add it to FS at
		/// the given dst name.
		/// delSrc indicates if the source should be removed
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void CopyFromLocalFile(bool delSrc, Path src, Path dst)
		{
			fs.CopyFromLocalFile(delSrc, src, dst);
		}

		/// <summary>The src files are on the local disk.</summary>
		/// <remarks>
		/// The src files are on the local disk.  Add it to FS at
		/// the given dst name.
		/// delSrc indicates if the source should be removed
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void CopyFromLocalFile(bool delSrc, bool overwrite, Path[] srcs, 
			Path dst)
		{
			fs.CopyFromLocalFile(delSrc, overwrite, srcs, dst);
		}

		/// <summary>The src file is on the local disk.</summary>
		/// <remarks>
		/// The src file is on the local disk.  Add it to FS at
		/// the given dst name.
		/// delSrc indicates if the source should be removed
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void CopyFromLocalFile(bool delSrc, bool overwrite, Path src, Path
			 dst)
		{
			fs.CopyFromLocalFile(delSrc, overwrite, src, dst);
		}

		/// <summary>The src file is under FS, and the dst is on the local disk.</summary>
		/// <remarks>
		/// The src file is under FS, and the dst is on the local disk.
		/// Copy it from FS control to the local dst name.
		/// delSrc indicates if the src will be removed or not.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void CopyToLocalFile(bool delSrc, Path src, Path dst)
		{
			fs.CopyToLocalFile(delSrc, src, dst);
		}

		/// <summary>Returns a local File that the user can write output to.</summary>
		/// <remarks>
		/// Returns a local File that the user can write output to.  The caller
		/// provides both the eventual FS target name and the local working
		/// file.  If the FS is local, we write directly into the target.  If
		/// the FS is remote, we write into the tmp local area.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override Path StartLocalOutput(Path fsOutputFile, Path tmpLocalFile)
		{
			return fs.StartLocalOutput(fsOutputFile, tmpLocalFile);
		}

		/// <summary>Called when we're all done writing to the target.</summary>
		/// <remarks>
		/// Called when we're all done writing to the target.  A local FS will
		/// do nothing, because we've written to exactly the right place.  A remote
		/// FS will copy the contents of tmpLocalFile to the correct target at
		/// fsOutputFile.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void CompleteLocalOutput(Path fsOutputFile, Path tmpLocalFile)
		{
			fs.CompleteLocalOutput(fsOutputFile, tmpLocalFile);
		}

		/// <summary>Return the total size of all files in the filesystem.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override long GetUsed()
		{
			return fs.GetUsed();
		}

		public override long GetDefaultBlockSize()
		{
			return fs.GetDefaultBlockSize();
		}

		public override short GetDefaultReplication()
		{
			return fs.GetDefaultReplication();
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsServerDefaults GetServerDefaults()
		{
			return fs.GetServerDefaults();
		}

		// path variants delegate to underlying filesystem 
		public override long GetDefaultBlockSize(Path f)
		{
			return fs.GetDefaultBlockSize(f);
		}

		public override short GetDefaultReplication(Path f)
		{
			return fs.GetDefaultReplication(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsServerDefaults GetServerDefaults(Path f)
		{
			return fs.GetServerDefaults(f);
		}

		/// <summary>Get file status.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileStatus(Path f)
		{
			return fs.GetFileStatus(f);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void Access(Path path, FsAction mode)
		{
			fs.Access(path, mode);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void CreateSymlink(Path target, Path link, bool createParent)
		{
			fs.CreateSymlink(target, link, createParent);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileLinkStatus(Path f)
		{
			return fs.GetFileLinkStatus(f);
		}

		public override bool SupportsSymlinks()
		{
			return fs.SupportsSymlinks();
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path GetLinkTarget(Path f)
		{
			return fs.GetLinkTarget(f);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override Path ResolveLink(Path f)
		{
			return fs.ResolveLink(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileChecksum GetFileChecksum(Path f)
		{
			return fs.GetFileChecksum(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileChecksum GetFileChecksum(Path f, long length)
		{
			return fs.GetFileChecksum(f, length);
		}

		public override void SetVerifyChecksum(bool verifyChecksum)
		{
			fs.SetVerifyChecksum(verifyChecksum);
		}

		public override void SetWriteChecksum(bool writeChecksum)
		{
			fs.SetWriteChecksum(writeChecksum);
		}

		public override Configuration GetConf()
		{
			return fs.GetConf();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			base.Close();
			fs.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetOwner(Path p, string username, string groupname)
		{
			fs.SetOwner(p, username, groupname);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetTimes(Path p, long mtime, long atime)
		{
			fs.SetTimes(p, mtime, atime);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetPermission(Path p, FsPermission permission)
		{
			fs.SetPermission(p, permission);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override FSDataOutputStream PrimitiveCreate(Path f, FsPermission
			 absolutePermission, EnumSet<CreateFlag> flag, int bufferSize, short replication
			, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt)
		{
			return fs.PrimitiveCreate(f, absolutePermission, flag, bufferSize, replication, blockSize
				, progress, checksumOpt);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override bool PrimitiveMkdir(Path f, FsPermission abdolutePermission
			)
		{
			return fs.PrimitiveMkdir(f, abdolutePermission);
		}

		public override FileSystem[] GetChildFileSystems()
		{
			// FileSystem
			return new FileSystem[] { fs };
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path CreateSnapshot(Path path, string snapshotName)
		{
			// FileSystem
			return fs.CreateSnapshot(path, snapshotName);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RenameSnapshot(Path path, string snapshotOldName, string snapshotNewName
			)
		{
			// FileSystem
			fs.RenameSnapshot(path, snapshotOldName, snapshotNewName);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DeleteSnapshot(Path path, string snapshotName)
		{
			// FileSystem
			fs.DeleteSnapshot(path, snapshotName);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ModifyAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			fs.ModifyAclEntries(path, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			fs.RemoveAclEntries(path, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveDefaultAcl(Path path)
		{
			fs.RemoveDefaultAcl(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAcl(Path path)
		{
			fs.RemoveAcl(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetAcl(Path path, IList<AclEntry> aclSpec)
		{
			fs.SetAcl(path, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public override AclStatus GetAclStatus(Path path)
		{
			return fs.GetAclStatus(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetXAttr(Path path, string name, byte[] value)
		{
			fs.SetXAttr(path, name, value);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetXAttr(Path path, string name, byte[] value, EnumSet<XAttrSetFlag
			> flag)
		{
			fs.SetXAttr(path, name, value, flag);
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] GetXAttr(Path path, string name)
		{
			return fs.GetXAttr(path, name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path path)
		{
			return fs.GetXAttrs(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path path, IList<string> names
			)
		{
			return fs.GetXAttrs(path, names);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> ListXAttrs(Path path)
		{
			return fs.ListXAttrs(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveXAttr(Path path, string name)
		{
			fs.RemoveXAttr(path, name);
		}
	}
}
