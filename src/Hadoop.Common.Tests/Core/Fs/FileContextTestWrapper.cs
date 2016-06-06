using System.IO;
using Hadoop.Common.Core.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>Helper class for unit tests.</summary>
	public sealed class FileContextTestWrapper : FSTestWrapper
	{
		private readonly FileContext fc;

		public FileContextTestWrapper(FileContext context)
			: this(context, null)
		{
		}

		public FileContextTestWrapper(FileContext context, string rootDir)
			: base(rootDir)
		{
			this.fc = context;
		}

		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		public override FSTestWrapper GetLocalFSWrapper()
		{
			return new Org.Apache.Hadoop.FS.FileContextTestWrapper(FileContext.GetLocalFSFileContext
				());
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path GetDefaultWorkingDirectory()
		{
			return GetTestRootPath("/user/" + Runtime.GetProperty("user.name")).MakeQualified
				(fc.GetDefaultFileSystem().GetUri(), fc.GetWorkingDirectory());
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
			FSDataOutputStream @out = fc.Create(path, EnumSet.Of(CreateFlag.Create), options);
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
			@out = fc.Create(path, EnumSet.Of(CreateFlag.Append));
			byte[] data = GetFileData(numBlocks, blockSize);
			@out.Write(data, 0, data.Length);
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Exists(Path p)
		{
			return fc.Util().Exists(p);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool IsFile(Path p)
		{
			try
			{
				return fc.GetFileStatus(p).IsFile();
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
				return fc.GetFileStatus(p).IsDirectory();
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
				return fc.GetFileLinkStatus(p).IsSymlink();
			}
			catch (FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void WriteFile(Path path, byte[] b)
		{
			FSDataOutputStream @out = fc.Create(path, EnumSet.Of(CreateFlag.Create), Options.CreateOpts
				.CreateParent());
			@out.Write(b);
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] ReadFile(Path path, int len)
		{
			DataInputStream dis = fc.Open(path);
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
			FileStatus s = fc.GetFileStatus(new Path(path));
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
			Assert.Equal(fc.MakeQualified(new Path(path)), s.GetPath());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CheckFileLinkStatus(string path, FSTestWrapper.FileType expectedType
			)
		{
			FileStatus s = fc.GetFileLinkStatus(new Path(path));
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
			Assert.Equal(fc.MakeQualified(new Path(path)), s.GetPath());
		}

		//
		// FileContext wrappers
		//
		public override Path MakeQualified(Path path)
		{
			return fc.MakeQualified(path);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void Mkdir(Path dir, FsPermission permission, bool createParent)
		{
			fc.Mkdir(dir, permission, createParent);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool Delete(Path f, bool recursive)
		{
			return fc.Delete(f, recursive);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileLinkStatus(Path f)
		{
			return fc.GetFileLinkStatus(f);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void CreateSymlink(Path target, Path link, bool createParent)
		{
			fc.CreateSymlink(target, link, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetWorkingDirectory(Path newWDir)
		{
			fc.SetWorkingDirectory(newWDir);
		}

		public override Path GetWorkingDirectory()
		{
			return fc.GetWorkingDirectory();
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileStatus(Path f)
		{
			return fc.GetFileStatus(f);
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
			return fc.Create(f, createFlag, opts);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FSDataInputStream Open(Path f)
		{
			return fc.Open(f);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool SetReplication(Path f, short replication)
		{
			return fc.SetReplication(f, replication);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetLinkTarget(Path f)
		{
			return fc.GetLinkTarget(f);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void Rename(Path src, Path dst, params Options.Rename[] options)
		{
			fc.Rename(src, dst, options);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override BlockLocation[] GetFileBlockLocations(Path f, long start, long len
			)
		{
			return fc.GetFileBlockLocations(f, start, len);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileChecksum GetFileChecksum(Path f)
		{
			return fc.GetFileChecksum(f);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override RemoteIterator<FileStatus> ListStatusIterator(Path f)
		{
			return fc.ListStatus(f);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void SetPermission(Path f, FsPermission permission)
		{
			fc.SetPermission(f, permission);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void SetOwner(Path f, string username, string groupname)
		{
			fc.SetOwner(f, username, groupname);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void SetTimes(Path f, long mtime, long atime)
		{
			fc.SetTimes(f, mtime, atime);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			return fc.Util().ListStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] GlobStatus(Path pathPattern, PathFilter filter)
		{
			return fc.Util().GlobStatus(pathPattern, filter);
		}
	}
}
