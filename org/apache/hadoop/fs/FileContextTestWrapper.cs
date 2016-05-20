using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Helper class for unit tests.</summary>
	public sealed class FileContextTestWrapper : org.apache.hadoop.fs.FSTestWrapper
	{
		private readonly org.apache.hadoop.fs.FileContext fc;

		public FileContextTestWrapper(org.apache.hadoop.fs.FileContext context)
			: this(context, null)
		{
		}

		public FileContextTestWrapper(org.apache.hadoop.fs.FileContext context, string rootDir
			)
			: base(rootDir)
		{
			this.fc = context;
		}

		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		public override org.apache.hadoop.fs.FSTestWrapper getLocalFSWrapper()
		{
			return new org.apache.hadoop.fs.FileContextTestWrapper(org.apache.hadoop.fs.FileContext
				.getLocalFSFileContext());
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path getDefaultWorkingDirectory()
		{
			return getTestRootPath("/user/" + Sharpen.Runtime.getProperty("user.name")).makeQualified
				(fc.getDefaultFileSystem().getUri(), fc.getWorkingDirectory());
		}

		/*
		* Create files with numBlocks blocks each with block size blockSize.
		*/
		/// <exception cref="System.IO.IOException"/>
		public override long createFile(org.apache.hadoop.fs.Path path, int numBlocks, params 
			org.apache.hadoop.fs.Options.CreateOpts[] options)
		{
			org.apache.hadoop.fs.Options.CreateOpts.BlockSize blockSizeOpt = org.apache.hadoop.fs.Options.CreateOpts
				.getOpt<org.apache.hadoop.fs.Options.CreateOpts.BlockSize>(options);
			long blockSize = blockSizeOpt != null ? blockSizeOpt.getValue() : DEFAULT_BLOCK_SIZE;
			org.apache.hadoop.fs.FSDataOutputStream @out = fc.create(path, java.util.EnumSet.
				of(org.apache.hadoop.fs.CreateFlag.CREATE), options);
			byte[] data = getFileData(numBlocks, blockSize);
			@out.write(data, 0, data.Length);
			@out.close();
			return data.Length;
		}

		/// <exception cref="System.IO.IOException"/>
		public override long createFile(org.apache.hadoop.fs.Path path, int numBlocks, int
			 blockSize)
		{
			return createFile(path, numBlocks, org.apache.hadoop.fs.Options.CreateOpts.blockSize
				(blockSize), org.apache.hadoop.fs.Options.CreateOpts.createParent());
		}

		/// <exception cref="System.IO.IOException"/>
		public override long createFile(org.apache.hadoop.fs.Path path)
		{
			return createFile(path, DEFAULT_NUM_BLOCKS, org.apache.hadoop.fs.Options.CreateOpts
				.createParent());
		}

		/// <exception cref="System.IO.IOException"/>
		public override long createFile(string name)
		{
			org.apache.hadoop.fs.Path path = getTestRootPath(name);
			return createFile(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override long createFileNonRecursive(string name)
		{
			org.apache.hadoop.fs.Path path = getTestRootPath(name);
			return createFileNonRecursive(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override long createFileNonRecursive(org.apache.hadoop.fs.Path path)
		{
			return createFile(path, DEFAULT_NUM_BLOCKS, org.apache.hadoop.fs.Options.CreateOpts
				.donotCreateParent());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void appendToFile(org.apache.hadoop.fs.Path path, int numBlocks, 
			params org.apache.hadoop.fs.Options.CreateOpts[] options)
		{
			org.apache.hadoop.fs.Options.CreateOpts.BlockSize blockSizeOpt = org.apache.hadoop.fs.Options.CreateOpts
				.getOpt<org.apache.hadoop.fs.Options.CreateOpts.BlockSize>(options);
			long blockSize = blockSizeOpt != null ? blockSizeOpt.getValue() : DEFAULT_BLOCK_SIZE;
			org.apache.hadoop.fs.FSDataOutputStream @out;
			@out = fc.create(path, java.util.EnumSet.of(org.apache.hadoop.fs.CreateFlag.APPEND
				));
			byte[] data = getFileData(numBlocks, blockSize);
			@out.write(data, 0, data.Length);
			@out.close();
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool exists(org.apache.hadoop.fs.Path p)
		{
			return fc.util().exists(p);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool isFile(org.apache.hadoop.fs.Path p)
		{
			try
			{
				return fc.getFileStatus(p).isFile();
			}
			catch (java.io.FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool isDir(org.apache.hadoop.fs.Path p)
		{
			try
			{
				return fc.getFileStatus(p).isDirectory();
			}
			catch (java.io.FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool isSymlink(org.apache.hadoop.fs.Path p)
		{
			try
			{
				return fc.getFileLinkStatus(p).isSymlink();
			}
			catch (java.io.FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void writeFile(org.apache.hadoop.fs.Path path, byte[] b)
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = fc.create(path, java.util.EnumSet.
				of(org.apache.hadoop.fs.CreateFlag.CREATE), org.apache.hadoop.fs.Options.CreateOpts
				.createParent());
			@out.write(b);
			@out.close();
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] readFile(org.apache.hadoop.fs.Path path, int len)
		{
			java.io.DataInputStream dis = fc.open(path);
			byte[] buffer = new byte[len];
			org.apache.hadoop.io.IOUtils.readFully(dis, buffer, 0, len);
			dis.close();
			return buffer;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus containsPath(org.apache.hadoop.fs.Path
			 path, org.apache.hadoop.fs.FileStatus[] dirList)
		{
			for (int i = 0; i < dirList.Length; i++)
			{
				if (path.Equals(dirList[i].getPath()))
				{
					return dirList[i];
				}
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus containsPath(string path, org.apache.hadoop.fs.FileStatus
			[] dirList)
		{
			return containsPath(new org.apache.hadoop.fs.Path(path), dirList);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void checkFileStatus(string path, org.apache.hadoop.fs.FSTestWrapper.fileType
			 expectedType)
		{
			org.apache.hadoop.fs.FileStatus s = fc.getFileStatus(new org.apache.hadoop.fs.Path
				(path));
			NUnit.Framework.Assert.IsNotNull(s);
			if (expectedType == org.apache.hadoop.fs.FSTestWrapper.fileType.isDir)
			{
				NUnit.Framework.Assert.IsTrue(s.isDirectory());
			}
			else
			{
				if (expectedType == org.apache.hadoop.fs.FSTestWrapper.fileType.isFile)
				{
					NUnit.Framework.Assert.IsTrue(s.isFile());
				}
				else
				{
					if (expectedType == org.apache.hadoop.fs.FSTestWrapper.fileType.isSymlink)
					{
						NUnit.Framework.Assert.IsTrue(s.isSymlink());
					}
				}
			}
			NUnit.Framework.Assert.AreEqual(fc.makeQualified(new org.apache.hadoop.fs.Path(path
				)), s.getPath());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void checkFileLinkStatus(string path, org.apache.hadoop.fs.FSTestWrapper.fileType
			 expectedType)
		{
			org.apache.hadoop.fs.FileStatus s = fc.getFileLinkStatus(new org.apache.hadoop.fs.Path
				(path));
			NUnit.Framework.Assert.IsNotNull(s);
			if (expectedType == org.apache.hadoop.fs.FSTestWrapper.fileType.isDir)
			{
				NUnit.Framework.Assert.IsTrue(s.isDirectory());
			}
			else
			{
				if (expectedType == org.apache.hadoop.fs.FSTestWrapper.fileType.isFile)
				{
					NUnit.Framework.Assert.IsTrue(s.isFile());
				}
				else
				{
					if (expectedType == org.apache.hadoop.fs.FSTestWrapper.fileType.isSymlink)
					{
						NUnit.Framework.Assert.IsTrue(s.isSymlink());
					}
				}
			}
			NUnit.Framework.Assert.AreEqual(fc.makeQualified(new org.apache.hadoop.fs.Path(path
				)), s.getPath());
		}

		//
		// FileContext wrappers
		//
		public override org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.Path
			 path)
		{
			return fc.makeQualified(path);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void mkdir(org.apache.hadoop.fs.Path dir, org.apache.hadoop.fs.permission.FsPermission
			 permission, bool createParent)
		{
			fc.mkdir(dir, permission, createParent);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool delete(org.apache.hadoop.fs.Path f, bool recursive)
		{
			return fc.delete(f, recursive);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return fc.getFileLinkStatus(f);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void createSymlink(org.apache.hadoop.fs.Path target, org.apache.hadoop.fs.Path
			 link, bool createParent)
		{
			fc.createSymlink(target, link, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setWorkingDirectory(org.apache.hadoop.fs.Path newWDir)
		{
			fc.setWorkingDirectory(newWDir);
		}

		public override org.apache.hadoop.fs.Path getWorkingDirectory()
		{
			return fc.getWorkingDirectory();
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return fc.getFileStatus(f);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> createFlag, params org.apache.hadoop.fs.Options.CreateOpts
			[] opts)
		{
			return fc.create(f, createFlag, opts);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f)
		{
			return fc.open(f);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool setReplication(org.apache.hadoop.fs.Path f, short replication
			)
		{
			return fc.setReplication(f, replication);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path
			 f)
		{
			return fc.getLinkTarget(f);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.ParentNotDirectoryException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst, params org.apache.hadoop.fs.Options.Rename[] options)
		{
			fc.rename(src, dst, options);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.Path
			 f, long start, long len)
		{
			return fc.getFileBlockLocations(f, start, len);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
			 f)
		{
			return fc.getFileChecksum(f);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus
			> listStatusIterator(org.apache.hadoop.fs.Path f)
		{
			return fc.listStatus(f);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void setPermission(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			fc.setPermission(f, permission);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void setOwner(org.apache.hadoop.fs.Path f, string username, string
			 groupname)
		{
			fc.setOwner(f, username, groupname);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void setTimes(org.apache.hadoop.fs.Path f, long mtime, long atime
			)
		{
			fc.setTimes(f, mtime, atime);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return fc.util().listStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus[] globStatus(org.apache.hadoop.fs.Path
			 pathPattern, org.apache.hadoop.fs.PathFilter filter)
		{
			return fc.util().globStatus(pathPattern, filter);
		}
	}
}
