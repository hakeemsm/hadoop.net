using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Helper class for unit tests.</summary>
	public sealed class FileSystemTestWrapper : org.apache.hadoop.fs.FSTestWrapper
	{
		private readonly org.apache.hadoop.fs.FileSystem fs;

		public FileSystemTestWrapper(org.apache.hadoop.fs.FileSystem fs)
			: this(fs, null)
		{
		}

		public FileSystemTestWrapper(org.apache.hadoop.fs.FileSystem fs, string rootDir)
			: base(rootDir)
		{
			this.fs = fs;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSTestWrapper getLocalFSWrapper()
		{
			return new org.apache.hadoop.fs.FileSystemTestWrapper(org.apache.hadoop.fs.FileSystem
				.getLocal(fs.getConf()));
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path getDefaultWorkingDirectory()
		{
			return getTestRootPath("/user/" + Sharpen.Runtime.getProperty("user.name")).makeQualified
				(fs.getUri(), fs.getWorkingDirectory());
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
			org.apache.hadoop.fs.FSDataOutputStream @out = create(path, java.util.EnumSet.of(
				org.apache.hadoop.fs.CreateFlag.CREATE), options);
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
			@out = fs.append(path);
			byte[] data = getFileData(numBlocks, blockSize);
			@out.write(data, 0, data.Length);
			@out.close();
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool exists(org.apache.hadoop.fs.Path p)
		{
			return fs.exists(p);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool isFile(org.apache.hadoop.fs.Path p)
		{
			try
			{
				return fs.getFileStatus(p).isFile();
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
				return fs.getFileStatus(p).isDirectory();
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
				return fs.getFileLinkStatus(p).isSymlink();
			}
			catch (java.io.FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void writeFile(org.apache.hadoop.fs.Path path, byte[] b)
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = create(path, java.util.EnumSet.of(
				org.apache.hadoop.fs.CreateFlag.CREATE), org.apache.hadoop.fs.Options.CreateOpts
				.createParent());
			@out.write(b);
			@out.close();
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] readFile(org.apache.hadoop.fs.Path path, int len)
		{
			java.io.DataInputStream dis = fs.open(path);
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
			org.apache.hadoop.fs.FileStatus s = fs.getFileStatus(new org.apache.hadoop.fs.Path
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
			NUnit.Framework.Assert.AreEqual(fs.makeQualified(new org.apache.hadoop.fs.Path(path
				)), s.getPath());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void checkFileLinkStatus(string path, org.apache.hadoop.fs.FSTestWrapper.fileType
			 expectedType)
		{
			org.apache.hadoop.fs.FileStatus s = fs.getFileLinkStatus(new org.apache.hadoop.fs.Path
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
			NUnit.Framework.Assert.AreEqual(fs.makeQualified(new org.apache.hadoop.fs.Path(path
				)), s.getPath());
		}

		//
		// FileContext wrappers
		//
		public override org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.Path
			 path)
		{
			return fs.makeQualified(path);
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
			fs.primitiveMkdir(dir, permission, createParent);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool delete(org.apache.hadoop.fs.Path f, bool recursive)
		{
			return fs.delete(f, recursive);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.getFileLinkStatus(f);
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
			fs.createSymlink(target, link, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void setWorkingDirectory(org.apache.hadoop.fs.Path newWDir)
		{
			fs.setWorkingDirectory(newWDir);
		}

		public override org.apache.hadoop.fs.Path getWorkingDirectory()
		{
			return fs.getWorkingDirectory();
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.getFileStatus(f);
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
			// Need to translate the FileContext-style options into FileSystem-style
			// Permissions with umask
			org.apache.hadoop.fs.Options.CreateOpts.Perms permOpt = org.apache.hadoop.fs.Options.CreateOpts
				.getOpt<org.apache.hadoop.fs.Options.CreateOpts.Perms>(opts);
			org.apache.hadoop.fs.permission.FsPermission umask = org.apache.hadoop.fs.permission.FsPermission
				.getUMask(fs.getConf());
			org.apache.hadoop.fs.permission.FsPermission permission = (permOpt != null) ? permOpt
				.getValue() : org.apache.hadoop.fs.permission.FsPermission.getFileDefault().applyUMask
				(umask);
			permission = permission.applyUMask(umask);
			// Overwrite
			bool overwrite = createFlag.contains(org.apache.hadoop.fs.CreateFlag.OVERWRITE);
			// bufferSize
			int bufferSize = fs.getConf().getInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic
				.IO_FILE_BUFFER_SIZE_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT
				);
			org.apache.hadoop.fs.Options.CreateOpts.BufferSize bufOpt = org.apache.hadoop.fs.Options.CreateOpts
				.getOpt<org.apache.hadoop.fs.Options.CreateOpts.BufferSize>(opts);
			bufferSize = (bufOpt != null) ? bufOpt.getValue() : bufferSize;
			// replication
			short replication = fs.getDefaultReplication(f);
			org.apache.hadoop.fs.Options.CreateOpts.ReplicationFactor repOpt = org.apache.hadoop.fs.Options.CreateOpts
				.getOpt<org.apache.hadoop.fs.Options.CreateOpts.ReplicationFactor>(opts);
			replication = (repOpt != null) ? repOpt.getValue() : replication;
			// blockSize
			long blockSize = fs.getDefaultBlockSize(f);
			org.apache.hadoop.fs.Options.CreateOpts.BlockSize blockOpt = org.apache.hadoop.fs.Options.CreateOpts
				.getOpt<org.apache.hadoop.fs.Options.CreateOpts.BlockSize>(opts);
			blockSize = (blockOpt != null) ? blockOpt.getValue() : blockSize;
			// Progressable
			org.apache.hadoop.util.Progressable progress = null;
			org.apache.hadoop.fs.Options.CreateOpts.Progress progressOpt = org.apache.hadoop.fs.Options.CreateOpts
				.getOpt<org.apache.hadoop.fs.Options.CreateOpts.Progress>(opts);
			progress = (progressOpt != null) ? progressOpt.getValue() : progress;
			return fs.create(f, permission, overwrite, bufferSize, replication, blockSize, progress
				);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.open(f);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.getLinkTarget(f);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override bool setReplication(org.apache.hadoop.fs.Path f, short replication
			)
		{
			return fs.setReplication(f, replication);
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
			fs.rename(src, dst, options);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.Path
			 f, long start, long len)
		{
			return fs.getFileBlockLocations(f, start, len);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.getFileChecksum(f);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus
			> listStatusIterator(org.apache.hadoop.fs.Path f)
		{
			return fs.listStatusIterator(f);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void setPermission(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			fs.setPermission(f, permission);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void setOwner(org.apache.hadoop.fs.Path f, string username, string
			 groupname)
		{
			fs.setOwner(f, username, groupname);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void setTimes(org.apache.hadoop.fs.Path f, long mtime, long atime
			)
		{
			fs.setTimes(f, mtime, atime);
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="java.io.FileNotFoundException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.listStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus[] globStatus(org.apache.hadoop.fs.Path
			 pathPattern, org.apache.hadoop.fs.PathFilter filter)
		{
			return fs.globStatus(pathPattern, filter);
		}
	}
}
