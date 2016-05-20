using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestFilterFileSystem
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.hadoop.fs.FileSystem
			.LOG;

		private static readonly org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		[NUnit.Framework.BeforeClass]
		public static void setup()
		{
			conf.set("fs.flfs.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestFilterFileSystem.FilterLocalFileSystem
				)).getName());
			conf.setBoolean("fs.flfs.impl.disable.cache", true);
			conf.setBoolean("fs.file.impl.disable.cache", true);
		}

		public class DontCheck
		{
			public virtual org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.Path
				 p, long start, long len)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FsServerDefaults getServerDefaults()
			{
				return null;
			}

			public virtual long getLength(org.apache.hadoop.fs.Path f)
			{
				return 0;
			}

			public virtual org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path
				 f)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path
				 f, int bufferSize)
			{
				return null;
			}

			public virtual void rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
				 dst, params org.apache.hadoop.fs.Options.Rename[] options)
			{
			}

			public virtual bool exists(org.apache.hadoop.fs.Path f)
			{
				return false;
			}

			public virtual bool isDirectory(org.apache.hadoop.fs.Path f)
			{
				return false;
			}

			public virtual bool isFile(org.apache.hadoop.fs.Path f)
			{
				return false;
			}

			public virtual bool createNewFile(org.apache.hadoop.fs.Path f)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
				 f, bool overwrite, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
				 f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, int
				 bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
				 f, org.apache.hadoop.fs.permission.FsPermission permission, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag
				> flags, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
				 f, org.apache.hadoop.fs.permission.FsPermission permission, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag
				> flags, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress, org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt)
			{
				return null;
			}

			public virtual bool mkdirs(org.apache.hadoop.fs.Path f)
			{
				return false;
			}

			public virtual org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
				 f)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
				 f)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
				 f, bool overwrite)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
				 f, org.apache.hadoop.util.Progressable progress)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
				 f, short replication)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
				 f, short replication, org.apache.hadoop.util.Progressable progress)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
				 f, bool overwrite, int bufferSize)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
				 f, bool overwrite, int bufferSize, org.apache.hadoop.util.Progressable progress
				)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
				 f, bool overwrite, int bufferSize, short replication, long blockSize)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
				 f, bool overwrite, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
				 f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, int
				 bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
				 f, org.apache.hadoop.fs.permission.FsPermission permission, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag
				> flags, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
				 f, org.apache.hadoop.fs.permission.FsPermission permission, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag
				> flags, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress, org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt)
			{
				return null;
			}

			public virtual string getName()
			{
				return null;
			}

			public virtual bool delete(org.apache.hadoop.fs.Path f)
			{
				return false;
			}

			public virtual short getReplication(org.apache.hadoop.fs.Path src)
			{
				return 0;
			}

			public virtual void processDeleteOnExit()
			{
			}

			public virtual org.apache.hadoop.fs.ContentSummary getContentSummary(org.apache.hadoop.fs.Path
				 f)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FsStatus getStatus()
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
				 f, org.apache.hadoop.fs.PathFilter filter)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
				[] files)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
				[] files, org.apache.hadoop.fs.PathFilter filter)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FileStatus[] globStatus(org.apache.hadoop.fs.Path
				 pathPattern)
			{
				return null;
			}

			public virtual org.apache.hadoop.fs.FileStatus[] globStatus(org.apache.hadoop.fs.Path
				 pathPattern, org.apache.hadoop.fs.PathFilter filter)
			{
				return null;
			}

			public virtual System.Collections.Generic.IEnumerator<org.apache.hadoop.fs.LocatedFileStatus
				> listFiles(org.apache.hadoop.fs.Path path, bool isRecursive)
			{
				return null;
			}

			public virtual System.Collections.Generic.IEnumerator<org.apache.hadoop.fs.LocatedFileStatus
				> listLocatedStatus(org.apache.hadoop.fs.Path f)
			{
				return null;
			}

			public virtual System.Collections.Generic.IEnumerator<org.apache.hadoop.fs.LocatedFileStatus
				> listLocatedStatus(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.PathFilter
				 filter)
			{
				return null;
			}

			public virtual void copyFromLocalFile(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
				 dst)
			{
			}

			public virtual void moveFromLocalFile(org.apache.hadoop.fs.Path[] srcs, org.apache.hadoop.fs.Path
				 dst)
			{
			}

			public virtual void moveFromLocalFile(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
				 dst)
			{
			}

			public virtual void copyToLocalFile(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
				 dst)
			{
			}

			public virtual void copyToLocalFile(bool delSrc, org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
				 dst, bool useRawLocalFileSystem)
			{
			}

			public virtual void moveToLocalFile(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
				 dst)
			{
			}

			public virtual long getBlockSize(org.apache.hadoop.fs.Path f)
			{
				return 0;
			}

			public virtual org.apache.hadoop.fs.FSDataOutputStream primitiveCreate(org.apache.hadoop.fs.Path
				 f, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> createFlag, params org.apache.hadoop.fs.Options.CreateOpts
				[] opts)
			{
				return null;
			}

			public virtual void primitiveMkdir(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
				 absolutePermission, bool createParent)
			{
			}

			public virtual int getDefaultPort()
			{
				return 0;
			}

			public virtual string getCanonicalServiceName()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.security.token.Token<object> getDelegationToken(
				string renewer)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool deleteOnExit(org.apache.hadoop.fs.Path f)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool cancelDeleteOnExit(org.apache.hadoop.fs.Path f)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.security.token.Token<object>[] addDelegationTokens
				(string renewer, org.apache.hadoop.security.Credentials creds)
			{
				return null;
			}

			public virtual string getScheme()
			{
				return "dontcheck";
			}

			public virtual org.apache.hadoop.fs.Path fixRelativePart(org.apache.hadoop.fs.Path
				 p)
			{
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFilterFileSystem()
		{
			foreach (java.lang.reflect.Method m in Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem
				)).getDeclaredMethods())
			{
				if (java.lang.reflect.Modifier.isStatic(m.getModifiers()))
				{
					continue;
				}
				if (java.lang.reflect.Modifier.isPrivate(m.getModifiers()))
				{
					continue;
				}
				if (java.lang.reflect.Modifier.isFinal(m.getModifiers()))
				{
					continue;
				}
				try
				{
					Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestFilterFileSystem.DontCheck
						)).getMethod(m.getName(), m.getParameterTypes());
					LOG.info("Skipping " + m);
				}
				catch (System.MissingMethodException)
				{
					LOG.info("Testing " + m);
					try
					{
						Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FilterFileSystem)).getDeclaredMethod
							(m.getName(), m.getParameterTypes());
					}
					catch (System.MissingMethodException exc2)
					{
						LOG.error("FilterFileSystem doesn't implement " + m);
						throw;
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFilterEmbedInit()
		{
			org.apache.hadoop.fs.FileSystem mockFs = createMockFs(false);
			// no conf = need init
			checkInit(new org.apache.hadoop.fs.FilterFileSystem(mockFs), true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFilterEmbedNoInit()
		{
			org.apache.hadoop.fs.FileSystem mockFs = createMockFs(true);
			// has conf = skip init
			checkInit(new org.apache.hadoop.fs.FilterFileSystem(mockFs), false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testLocalEmbedInit()
		{
			org.apache.hadoop.fs.FileSystem mockFs = createMockFs(false);
			// no conf = need init
			checkInit(new org.apache.hadoop.fs.LocalFileSystem(mockFs), true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testLocalEmbedNoInit()
		{
			org.apache.hadoop.fs.FileSystem mockFs = createMockFs(true);
			// has conf = skip init
			checkInit(new org.apache.hadoop.fs.LocalFileSystem(mockFs), false);
		}

		private org.apache.hadoop.fs.FileSystem createMockFs(bool useConf)
		{
			org.apache.hadoop.fs.FileSystem mockFs = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileSystem
				>();
			org.mockito.Mockito.when(mockFs.getUri()).thenReturn(java.net.URI.create("mock:/"
				));
			org.mockito.Mockito.when(mockFs.getConf()).thenReturn(useConf ? conf : null);
			return mockFs;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetLocalFsSetsConfs()
		{
			org.apache.hadoop.fs.LocalFileSystem lfs = org.apache.hadoop.fs.FileSystem.getLocal
				(conf);
			checkFsConf(lfs, conf, 2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetFilterLocalFsSetsConfs()
		{
			org.apache.hadoop.fs.FilterFileSystem flfs = (org.apache.hadoop.fs.FilterFileSystem
				)org.apache.hadoop.fs.FileSystem.get(java.net.URI.create("flfs:/"), conf);
			checkFsConf(flfs, conf, 3);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInitLocalFsSetsConfs()
		{
			org.apache.hadoop.fs.LocalFileSystem lfs = new org.apache.hadoop.fs.LocalFileSystem
				();
			checkFsConf(lfs, null, 2);
			lfs.initialize(lfs.getUri(), conf);
			checkFsConf(lfs, conf, 2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInitFilterFsSetsEmbedConf()
		{
			org.apache.hadoop.fs.LocalFileSystem lfs = new org.apache.hadoop.fs.LocalFileSystem
				();
			checkFsConf(lfs, null, 2);
			org.apache.hadoop.fs.FilterFileSystem ffs = new org.apache.hadoop.fs.FilterFileSystem
				(lfs);
			NUnit.Framework.Assert.AreEqual(lfs, ffs.getRawFileSystem());
			checkFsConf(ffs, null, 3);
			ffs.initialize(java.net.URI.create("filter:/"), conf);
			checkFsConf(ffs, conf, 3);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInitFilterLocalFsSetsEmbedConf()
		{
			org.apache.hadoop.fs.FilterFileSystem flfs = new org.apache.hadoop.fs.TestFilterFileSystem.FilterLocalFileSystem
				();
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.LocalFileSystem
				)), Sharpen.Runtime.getClassForObject(flfs.getRawFileSystem()));
			checkFsConf(flfs, null, 3);
			flfs.initialize(java.net.URI.create("flfs:/"), conf);
			checkFsConf(flfs, conf, 3);
		}

		[NUnit.Framework.Test]
		public virtual void testVerifyChecksumPassthru()
		{
			org.apache.hadoop.fs.FileSystem mockFs = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileSystem
				>();
			org.apache.hadoop.fs.FileSystem fs = new org.apache.hadoop.fs.FilterFileSystem(mockFs
				);
			fs.setVerifyChecksum(false);
			org.mockito.Mockito.verify(mockFs).setVerifyChecksum(org.mockito.Matchers.eq(false
				));
			org.mockito.Mockito.reset(mockFs);
			fs.setVerifyChecksum(true);
			org.mockito.Mockito.verify(mockFs).setVerifyChecksum(org.mockito.Matchers.eq(true
				));
		}

		[NUnit.Framework.Test]
		public virtual void testWriteChecksumPassthru()
		{
			org.apache.hadoop.fs.FileSystem mockFs = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileSystem
				>();
			org.apache.hadoop.fs.FileSystem fs = new org.apache.hadoop.fs.FilterFileSystem(mockFs
				);
			fs.setWriteChecksum(false);
			org.mockito.Mockito.verify(mockFs).setWriteChecksum(org.mockito.Matchers.eq(false
				));
			org.mockito.Mockito.reset(mockFs);
			fs.setWriteChecksum(true);
			org.mockito.Mockito.verify(mockFs).setWriteChecksum(org.mockito.Matchers.eq(true)
				);
		}

		/// <exception cref="System.Exception"/>
		private void checkInit(org.apache.hadoop.fs.FilterFileSystem fs, bool expectInit)
		{
			java.net.URI uri = java.net.URI.create("filter:/");
			fs.initialize(uri, conf);
			org.apache.hadoop.fs.FileSystem embedFs = fs.getRawFileSystem();
			if (expectInit)
			{
				org.mockito.Mockito.verify(embedFs, org.mockito.Mockito.times(1)).initialize(org.mockito.Matchers.eq
					(uri), org.mockito.Matchers.eq(conf));
			}
			else
			{
				org.mockito.Mockito.verify(embedFs, org.mockito.Mockito.times(0)).initialize(org.mockito.Matchers.any
					<java.net.URI>(), org.mockito.Matchers.any<org.apache.hadoop.conf.Configuration>
					());
			}
		}

		// check the given fs's conf, and all its filtered filesystems
		private void checkFsConf(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.conf.Configuration
			 conf, int expectDepth)
		{
			int depth = 0;
			while (true)
			{
				depth++;
				NUnit.Framework.Assert.IsFalse("depth " + depth + ">" + expectDepth, depth > expectDepth
					);
				NUnit.Framework.Assert.AreEqual(conf, fs.getConf());
				if (!(fs is org.apache.hadoop.fs.FilterFileSystem))
				{
					break;
				}
				fs = ((org.apache.hadoop.fs.FilterFileSystem)fs).getRawFileSystem();
			}
			NUnit.Framework.Assert.AreEqual(expectDepth, depth);
		}

		private class FilterLocalFileSystem : org.apache.hadoop.fs.FilterFileSystem
		{
			internal FilterLocalFileSystem()
				: base(new org.apache.hadoop.fs.LocalFileSystem())
			{
			}
		}
	}
}
