using System;
using System.Collections.Generic;
using System.Reflection;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Mockito;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.FS
{
	public class TestFilterFileSystem
	{
		private static readonly Log Log = FileSystem.Log;

		private static readonly Configuration conf = new Configuration();

		[BeforeClass]
		public static void Setup()
		{
			conf.Set("fs.flfs.impl", typeof(TestFilterFileSystem.FilterLocalFileSystem).FullName
				);
			conf.SetBoolean("fs.flfs.impl.disable.cache", true);
			conf.SetBoolean("fs.file.impl.disable.cache", true);
		}

		public class DontCheck
		{
			public virtual BlockLocation[] GetFileBlockLocations(Path p, long start, long len
				)
			{
				return null;
			}

			public virtual FsServerDefaults GetServerDefaults()
			{
				return null;
			}

			public virtual long GetLength(Path f)
			{
				return 0;
			}

			public virtual FSDataOutputStream Append(Path f)
			{
				return null;
			}

			public virtual FSDataOutputStream Append(Path f, int bufferSize)
			{
				return null;
			}

			public virtual void Rename(Path src, Path dst, params Options.Rename[] options)
			{
			}

			public virtual bool Exists(Path f)
			{
				return false;
			}

			public virtual bool IsDirectory(Path f)
			{
				return false;
			}

			public virtual bool IsFile(Path f)
			{
				return false;
			}

			public virtual bool CreateNewFile(Path f)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FSDataOutputStream CreateNonRecursive(Path f, bool overwrite, int 
				bufferSize, short replication, long blockSize, Progressable progress)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FSDataOutputStream CreateNonRecursive(Path f, FsPermission permission
				, bool overwrite, int bufferSize, short replication, long blockSize, Progressable
				 progress)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FSDataOutputStream CreateNonRecursive(Path f, FsPermission permission
				, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, 
				Progressable progress)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FSDataOutputStream CreateNonRecursive(Path f, FsPermission permission
				, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, 
				Progressable progress, Options.ChecksumOpt checksumOpt)
			{
				return null;
			}

			public virtual bool Mkdirs(Path f)
			{
				return false;
			}

			public virtual FSDataInputStream Open(Path f)
			{
				return null;
			}

			public virtual FSDataOutputStream Create(Path f)
			{
				return null;
			}

			public virtual FSDataOutputStream Create(Path f, bool overwrite)
			{
				return null;
			}

			public virtual FSDataOutputStream Create(Path f, Progressable progress)
			{
				return null;
			}

			public virtual FSDataOutputStream Create(Path f, short replication)
			{
				return null;
			}

			public virtual FSDataOutputStream Create(Path f, short replication, Progressable 
				progress)
			{
				return null;
			}

			public virtual FSDataOutputStream Create(Path f, bool overwrite, int bufferSize)
			{
				return null;
			}

			public virtual FSDataOutputStream Create(Path f, bool overwrite, int bufferSize, 
				Progressable progress)
			{
				return null;
			}

			public virtual FSDataOutputStream Create(Path f, bool overwrite, int bufferSize, 
				short replication, long blockSize)
			{
				return null;
			}

			public virtual FSDataOutputStream Create(Path f, bool overwrite, int bufferSize, 
				short replication, long blockSize, Progressable progress)
			{
				return null;
			}

			public virtual FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
				, int bufferSize, short replication, long blockSize, Progressable progress)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FSDataOutputStream Create(Path f, FsPermission permission, EnumSet
				<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable
				 progress)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FSDataOutputStream Create(Path f, FsPermission permission, EnumSet
				<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable
				 progress, Options.ChecksumOpt checksumOpt)
			{
				return null;
			}

			public virtual string GetName()
			{
				return null;
			}

			public virtual bool Delete(Path f)
			{
				return false;
			}

			public virtual short GetReplication(Path src)
			{
				return 0;
			}

			public virtual void ProcessDeleteOnExit()
			{
			}

			public virtual ContentSummary GetContentSummary(Path f)
			{
				return null;
			}

			public virtual FsStatus GetStatus()
			{
				return null;
			}

			public virtual FileStatus[] ListStatus(Path f, PathFilter filter)
			{
				return null;
			}

			public virtual FileStatus[] ListStatus(Path[] files)
			{
				return null;
			}

			public virtual FileStatus[] ListStatus(Path[] files, PathFilter filter)
			{
				return null;
			}

			public virtual FileStatus[] GlobStatus(Path pathPattern)
			{
				return null;
			}

			public virtual FileStatus[] GlobStatus(Path pathPattern, PathFilter filter)
			{
				return null;
			}

			public virtual IEnumerator<LocatedFileStatus> ListFiles(Path path, bool isRecursive
				)
			{
				return null;
			}

			public virtual IEnumerator<LocatedFileStatus> ListLocatedStatus(Path f)
			{
				return null;
			}

			public virtual IEnumerator<LocatedFileStatus> ListLocatedStatus(Path f, PathFilter
				 filter)
			{
				return null;
			}

			public virtual void CopyFromLocalFile(Path src, Path dst)
			{
			}

			public virtual void MoveFromLocalFile(Path[] srcs, Path dst)
			{
			}

			public virtual void MoveFromLocalFile(Path src, Path dst)
			{
			}

			public virtual void CopyToLocalFile(Path src, Path dst)
			{
			}

			public virtual void CopyToLocalFile(bool delSrc, Path src, Path dst, bool useRawLocalFileSystem
				)
			{
			}

			public virtual void MoveToLocalFile(Path src, Path dst)
			{
			}

			public virtual long GetBlockSize(Path f)
			{
				return 0;
			}

			public virtual FSDataOutputStream PrimitiveCreate(Path f, EnumSet<CreateFlag> createFlag
				, params Options.CreateOpts[] opts)
			{
				return null;
			}

			public virtual void PrimitiveMkdir(Path f, FsPermission absolutePermission, bool 
				createParent)
			{
			}

			public virtual int GetDefaultPort()
			{
				return 0;
			}

			public virtual string GetCanonicalServiceName()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual Org.Apache.Hadoop.Security.Token.Token<object> GetDelegationToken(
				string renewer)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool DeleteOnExit(Path f)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool CancelDeleteOnExit(Path f)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual Org.Apache.Hadoop.Security.Token.Token<object>[] AddDelegationTokens
				(string renewer, Credentials creds)
			{
				return null;
			}

			public virtual string GetScheme()
			{
				return "dontcheck";
			}

			public virtual Path FixRelativePart(Path p)
			{
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFilterFileSystem()
		{
			foreach (MethodInfo m in Sharpen.Runtime.GetDeclaredMethods(typeof(FileSystem)))
			{
				if (Modifier.IsStatic(m.GetModifiers()))
				{
					continue;
				}
				if (Modifier.IsPrivate(m.GetModifiers()))
				{
					continue;
				}
				if (Modifier.IsFinal(m.GetModifiers()))
				{
					continue;
				}
				try
				{
					typeof(TestFilterFileSystem.DontCheck).GetMethod(m.Name, Sharpen.Runtime.GetParameterTypes
						(m));
					Log.Info("Skipping " + m);
				}
				catch (MissingMethodException)
				{
					Log.Info("Testing " + m);
					try
					{
						Sharpen.Runtime.GetDeclaredMethod(typeof(FilterFileSystem), m.Name, Sharpen.Runtime.GetParameterTypes
							(m));
					}
					catch (MissingMethodException exc2)
					{
						Log.Error("FilterFileSystem doesn't implement " + m);
						throw;
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFilterEmbedInit()
		{
			FileSystem mockFs = CreateMockFs(false);
			// no conf = need init
			CheckInit(new FilterFileSystem(mockFs), true);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFilterEmbedNoInit()
		{
			FileSystem mockFs = CreateMockFs(true);
			// has conf = skip init
			CheckInit(new FilterFileSystem(mockFs), false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestLocalEmbedInit()
		{
			FileSystem mockFs = CreateMockFs(false);
			// no conf = need init
			CheckInit(new LocalFileSystem(mockFs), true);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestLocalEmbedNoInit()
		{
			FileSystem mockFs = CreateMockFs(true);
			// has conf = skip init
			CheckInit(new LocalFileSystem(mockFs), false);
		}

		private FileSystem CreateMockFs(bool useConf)
		{
			FileSystem mockFs = Org.Mockito.Mockito.Mock<FileSystem>();
			Org.Mockito.Mockito.When(mockFs.GetUri()).ThenReturn(URI.Create("mock:/"));
			Org.Mockito.Mockito.When(mockFs.GetConf()).ThenReturn(useConf ? conf : null);
			return mockFs;
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetLocalFsSetsConfs()
		{
			LocalFileSystem lfs = FileSystem.GetLocal(conf);
			CheckFsConf(lfs, conf, 2);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetFilterLocalFsSetsConfs()
		{
			FilterFileSystem flfs = (FilterFileSystem)FileSystem.Get(URI.Create("flfs:/"), conf
				);
			CheckFsConf(flfs, conf, 3);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestInitLocalFsSetsConfs()
		{
			LocalFileSystem lfs = new LocalFileSystem();
			CheckFsConf(lfs, null, 2);
			lfs.Initialize(lfs.GetUri(), conf);
			CheckFsConf(lfs, conf, 2);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestInitFilterFsSetsEmbedConf()
		{
			LocalFileSystem lfs = new LocalFileSystem();
			CheckFsConf(lfs, null, 2);
			FilterFileSystem ffs = new FilterFileSystem(lfs);
			Assert.Equal(lfs, ffs.GetRawFileSystem());
			CheckFsConf(ffs, null, 3);
			ffs.Initialize(URI.Create("filter:/"), conf);
			CheckFsConf(ffs, conf, 3);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestInitFilterLocalFsSetsEmbedConf()
		{
			FilterFileSystem flfs = new TestFilterFileSystem.FilterLocalFileSystem();
			Assert.Equal(typeof(LocalFileSystem), flfs.GetRawFileSystem().
				GetType());
			CheckFsConf(flfs, null, 3);
			flfs.Initialize(URI.Create("flfs:/"), conf);
			CheckFsConf(flfs, conf, 3);
		}

		[Fact]
		public virtual void TestVerifyChecksumPassthru()
		{
			FileSystem mockFs = Org.Mockito.Mockito.Mock<FileSystem>();
			FileSystem fs = new FilterFileSystem(mockFs);
			fs.SetVerifyChecksum(false);
			Org.Mockito.Mockito.Verify(mockFs).SetVerifyChecksum(Matchers.Eq(false));
			Org.Mockito.Mockito.Reset(mockFs);
			fs.SetVerifyChecksum(true);
			Org.Mockito.Mockito.Verify(mockFs).SetVerifyChecksum(Matchers.Eq(true));
		}

		[Fact]
		public virtual void TestWriteChecksumPassthru()
		{
			FileSystem mockFs = Org.Mockito.Mockito.Mock<FileSystem>();
			FileSystem fs = new FilterFileSystem(mockFs);
			fs.SetWriteChecksum(false);
			Org.Mockito.Mockito.Verify(mockFs).SetWriteChecksum(Matchers.Eq(false));
			Org.Mockito.Mockito.Reset(mockFs);
			fs.SetWriteChecksum(true);
			Org.Mockito.Mockito.Verify(mockFs).SetWriteChecksum(Matchers.Eq(true));
		}

		/// <exception cref="System.Exception"/>
		private void CheckInit(FilterFileSystem fs, bool expectInit)
		{
			URI uri = URI.Create("filter:/");
			fs.Initialize(uri, conf);
			FileSystem embedFs = fs.GetRawFileSystem();
			if (expectInit)
			{
				Org.Mockito.Mockito.Verify(embedFs, Org.Mockito.Mockito.Times(1)).Initialize(Matchers.Eq
					(uri), Matchers.Eq(conf));
			}
			else
			{
				Org.Mockito.Mockito.Verify(embedFs, Org.Mockito.Mockito.Times(0)).Initialize(Matchers.Any
					<URI>(), Matchers.Any<Configuration>());
			}
		}

		// check the given fs's conf, and all its filtered filesystems
		private void CheckFsConf(FileSystem fs, Configuration conf, int expectDepth)
		{
			int depth = 0;
			while (true)
			{
				depth++;
				NUnit.Framework.Assert.IsFalse("depth " + depth + ">" + expectDepth, depth > expectDepth
					);
				Assert.Equal(conf, fs.GetConf());
				if (!(fs is FilterFileSystem))
				{
					break;
				}
				fs = ((FilterFileSystem)fs).GetRawFileSystem();
			}
			Assert.Equal(expectDepth, depth);
		}

		private class FilterLocalFileSystem : FilterFileSystem
		{
			internal FilterLocalFileSystem()
				: base(new LocalFileSystem())
			{
			}
		}
	}
}
