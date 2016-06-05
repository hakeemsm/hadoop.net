using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>Helper class for unit tests.</summary>
	public sealed class FileSystemTestWrapper : FSTestWrapper
	{
		private readonly FileSystem fs;

		public FileSystemTestWrapper(FileSystem fs)
			: this(fs, null)
		{
		}

		public FileSystemTestWrapper(FileSystem fs, string rootDir)
			: base(rootDir)
		{
			this.fs = fs;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSTestWrapper GetLocalFSWrapper()
		{
			return new Org.Apache.Hadoop.FS.FileSystemTestWrapper(FileSystem.GetLocal(fs.GetConf
				()));
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path GetDefaultWorkingDirectory()
		{
			return GetTestRootPath("/user/" + Runtime.GetProperty("user.name")).MakeQualified
				(fs.GetUri(), fs.GetWorkingDirectory());
		}

		/*
		* Create files with numBlocks blocks each with block size blockSize.
		*/
		/// <exception cref="System.IO.IOException"/>
		public override long CreateFile(Path path, int numBlocks, params Options.CreateOpts
			[] options)
		{
			Options.CreateOpts.BlockSize blockSizeOpt = Options.CreateOpts.GetOpt<Options.CreateOpts.BlockSize
				>(options);
			long blockSize = blockSizeOpt != null ? blockSizeOpt.GetValue() : DefaultBlockSize;
			FSDataOutputStream @out = Create(path, EnumSet.Of(CreateFlag.Create), options);
			byte[] data = GetFileData(numBlocks, blockSize);
			@out.Write(data, 0, data.Length);
			@out.Close();
			return data.Length;
		}

		/// <exception cref="System.IO.IOException"/>
		public override long CreateFile(Path path, int numBlocks, int blockSize)
		{
			return CreateFile(path, numBlocks, Options.CreateOpts.BlockSize(blockSize), Options.CreateOpts
				.CreateParent());
		}

		/// <exception cref="System.IO.IOException"/>
		public override long CreateFile(Path path)
		{
			return CreateFile(path, DefaultNumBlocks, Options.CreateOpts.CreateParent());
		}

		/// <exception cref="System.IO.IOException"/>
		public override long CreateFile(string name)
		{
			Path path = GetTestRootPath(name);
			return CreateFile(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override long CreateFileNonRecursive(string name)
		{
			Path path = GetTestRootPath(name);
			return CreateFileNonRecursive(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override long CreateFileNonRecursive(Path path)
		{
			return CreateFile(path, DefaultNumBlocks, Options.CreateOpts.DonotCreateParent());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void AppendToFile(Path path, int numBlocks, params Options.CreateOpts
			[] options)
		{
			Options.CreateOpts.BlockSize blockSizeOpt = Options.CreateOpts.GetOpt<Options.CreateOpts.BlockSize
				>(options);
			long blockSize = blockSizeOpt != null ? blockSizeOpt.GetValue() : DefaultBlockSize;
			FSDataOutputStream @out;
			@out = fs.Append(path);
			byte[] data = GetFileData(numBlocks, blockSize);
			@out.Write(data, 0, data.Length);
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Exists(Path p)
		{
			return fs.Exists(p);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool IsFile(Path p)
		{
			try
			{
				return fs.GetFileStatus(p).IsFile();
			}
			catch (FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool IsDir(Path p)
		{
			try
			{
				return fs.GetFileStatus(p).IsDirectory();
			}
			catch (FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool IsSymlink(Path p)
		{
			try
			{
				return fs.GetFileLinkStatus(p).IsSymlink();
			}
			catch (FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void WriteFile(Path path, byte[] b)
		{
			FSDataOutputStream @out = Create(path, EnumSet.Of(CreateFlag.Create), Options.CreateOpts
				.CreateParent());
			@out.Write(b);
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] ReadFile(Path path, int len)
		{
			DataInputStream dis = fs.Open(path);
			byte[] buffer = new byte[len];
			IOUtils.ReadFully(dis, buffer, 0, len);
			dis.Close();
			return buffer;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus ContainsPath(Path path, FileStatus[] dirList)
		{
			for (int i = 0; i < dirList.Length; i++)
			{
				if (path.Equals(dirList[i].GetPath()))
				{
					return dirList[i];
				}
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus ContainsPath(string path, FileStatus[] dirList)
		{
			return ContainsPath(new Path(path), dirList);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CheckFileStatus(string path, FSTestWrapper.FileType expectedType
			)
		{
			FileStatus s = fs.GetFileStatus(new Path(path));
			NUnit.Framework.Assert.IsNotNull(s);
			if (expectedType == FSTestWrapper.FileType.isDir)
			{
				Assert.True(s.IsDirectory());
			}
			else
			{
				if (expectedType == FSTestWrapper.FileType.isFile)
				{
					Assert.True(s.IsFile());
				}
				else
				{
					if (expectedType == FSTestWrapper.FileType.isSymlink)
					{
						Assert.True(s.IsSymlink());
					}
				}
			}
			Assert.Equal(fs.MakeQualified(new Path(path)), s.GetPath());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CheckFileLinkStatus(string path, FSTestWrapper.FileType expectedType
			)
		{
			FileStatus s = fs.GetFileLinkStatus(new Path(path));
			NUnit.Framework.Assert.IsNotNull(s);
			if (expectedType == FSTestWrapper.FileType.isDir)
			{
				Assert.True(s.IsDirectory());
			}
			else
			{
				if (expectedType == FSTestWrapper.FileType.isFile)
				{
					Assert.True(s.IsFile());
				}
				else
				{
					if (expectedType == FSTestWrapper.FileType.isSymlink)
					{
						Assert.True(s.IsSymlink());
					}
				}
			}
			Assert.Equal(fs.MakeQualified(new Path(path)), s.GetPath());
		}

		//
		// FileContext wrappers
		//
		public override Path MakeQualified(Path path)
		{
			return fs.MakeQualified(path);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void Mkdir(Path dir, FsPermission permission, bool createParent)
		{
			fs.PrimitiveMkdir(dir, permission, createParent);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool Delete(Path f, bool recursive)
		{
			return fs.Delete(f, recursive);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileLinkStatus(Path f)
		{
			return fs.GetFileLinkStatus(f);
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

		/// <exception cref="System.IO.IOException"/>
		public override void SetWorkingDirectory(Path newWDir)
		{
			fs.SetWorkingDirectory(newWDir);
		}

		public override Path GetWorkingDirectory()
		{
			return fs.GetWorkingDirectory();
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileStatus(Path f)
		{
			return fs.GetFileStatus(f);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Create(Path f, EnumSet<CreateFlag> createFlag, 
			params Options.CreateOpts[] opts)
		{
			// Need to translate the FileContext-style options into FileSystem-style
			// Permissions with umask
			Options.CreateOpts.Perms permOpt = Options.CreateOpts.GetOpt<Options.CreateOpts.Perms
				>(opts);
			FsPermission umask = FsPermission.GetUMask(fs.GetConf());
			FsPermission permission = (permOpt != null) ? permOpt.GetValue() : FsPermission.GetFileDefault
				().ApplyUMask(umask);
			permission = permission.ApplyUMask(umask);
			// Overwrite
			bool overwrite = createFlag.Contains(CreateFlag.Overwrite);
			// bufferSize
			int bufferSize = fs.GetConf().GetInt(CommonConfigurationKeysPublic.IoFileBufferSizeKey
				, CommonConfigurationKeysPublic.IoFileBufferSizeDefault);
			Options.CreateOpts.BufferSize bufOpt = Options.CreateOpts.GetOpt<Options.CreateOpts.BufferSize
				>(opts);
			bufferSize = (bufOpt != null) ? bufOpt.GetValue() : bufferSize;
			// replication
			short replication = fs.GetDefaultReplication(f);
			Options.CreateOpts.ReplicationFactor repOpt = Options.CreateOpts.GetOpt<Options.CreateOpts.ReplicationFactor
				>(opts);
			replication = (repOpt != null) ? repOpt.GetValue() : replication;
			// blockSize
			long blockSize = fs.GetDefaultBlockSize(f);
			Options.CreateOpts.BlockSize blockOpt = Options.CreateOpts.GetOpt<Options.CreateOpts.BlockSize
				>(opts);
			blockSize = (blockOpt != null) ? blockOpt.GetValue() : blockSize;
			// Progressable
			Progressable progress = null;
			Options.CreateOpts.Progress progressOpt = Options.CreateOpts.GetOpt<Options.CreateOpts.Progress
				>(opts);
			progress = (progressOpt != null) ? progressOpt.GetValue() : progress;
			return fs.Create(f, permission, overwrite, bufferSize, replication, blockSize, progress
				);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FSDataInputStream Open(Path f)
		{
			return fs.Open(f);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetLinkTarget(Path f)
		{
			return fs.GetLinkTarget(f);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool SetReplication(Path f, short replication)
		{
			return fs.SetReplication(f, replication);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void Rename(Path src, Path dst, params Options.Rename[] options)
		{
			fs.Rename(src, dst, options);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override BlockLocation[] GetFileBlockLocations(Path f, long start, long len
			)
		{
			return fs.GetFileBlockLocations(f, start, len);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileChecksum GetFileChecksum(Path f)
		{
			return fs.GetFileChecksum(f);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override RemoteIterator<FileStatus> ListStatusIterator(Path f)
		{
			return fs.ListStatusIterator(f);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void SetPermission(Path f, FsPermission permission)
		{
			fs.SetPermission(f, permission);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void SetOwner(Path f, string username, string groupname)
		{
			fs.SetOwner(f, username, groupname);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void SetTimes(Path f, long mtime, long atime)
		{
			fs.SetTimes(f, mtime, atime);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			return fs.ListStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] GlobStatus(Path pathPattern, PathFilter filter)
		{
			return fs.GlobStatus(pathPattern, filter);
		}
	}
}
