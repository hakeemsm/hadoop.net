using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// This test class checks basic operations with
	/// <see cref="HarFileSystem"/>
	/// including
	/// various initialization cases, getters, and modification methods.
	/// NB: to run this test from an IDE make sure the folder
	/// "hadoop-common-project/hadoop-common/src/main/resources/" is added as a
	/// source path. This will allow the system to pick up the "core-default.xml" and
	/// "META-INF/services/..." resources from the class-path in the runtime.
	/// </summary>
	public class TestHarFileSystemBasics
	{
		private static readonly string ROOT_PATH = Sharpen.Runtime.getProperty("test.build.data"
			, "build/test/data");

		private static readonly org.apache.hadoop.fs.Path rootPath;

		static TestHarFileSystemBasics()
		{
			string root = new org.apache.hadoop.fs.Path(new java.io.File(ROOT_PATH).getAbsolutePath
				(), "localfs").toUri().getPath();
			// Strip drive specifier on Windows, which would make the HAR URI invalid and
			// cause tests to fail.
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				root = Sharpen.Runtime.substring(root, root.IndexOf(':') + 1);
			}
			rootPath = new org.apache.hadoop.fs.Path(root);
		}

		private static readonly org.apache.hadoop.fs.Path harPath = new org.apache.hadoop.fs.Path
			(rootPath, "path1/path2/my.har");

		private org.apache.hadoop.fs.FileSystem localFileSystem;

		private org.apache.hadoop.fs.HarFileSystem harFileSystem;

		private org.apache.hadoop.conf.Configuration conf;

		// NB: .har suffix is necessary
		/*
		* creates and returns fully initialized HarFileSystem
		*/
		/// <exception cref="System.Exception"/>
		private org.apache.hadoop.fs.HarFileSystem createHarFileSystem(org.apache.hadoop.conf.Configuration
			 conf)
		{
			localFileSystem = org.apache.hadoop.fs.FileSystem.getLocal(conf);
			localFileSystem.initialize(new java.net.URI("file:///"), conf);
			localFileSystem.mkdirs(rootPath);
			localFileSystem.mkdirs(harPath);
			org.apache.hadoop.fs.Path indexPath = new org.apache.hadoop.fs.Path(harPath, "_index"
				);
			org.apache.hadoop.fs.Path masterIndexPath = new org.apache.hadoop.fs.Path(harPath
				, "_masterindex");
			localFileSystem.createNewFile(indexPath);
			NUnit.Framework.Assert.IsTrue(localFileSystem.exists(indexPath));
			localFileSystem.createNewFile(masterIndexPath);
			NUnit.Framework.Assert.IsTrue(localFileSystem.exists(masterIndexPath));
			writeVersionToMasterIndexImpl(org.apache.hadoop.fs.HarFileSystem.VERSION, masterIndexPath
				);
			org.apache.hadoop.fs.HarFileSystem harFileSystem = new org.apache.hadoop.fs.HarFileSystem
				(localFileSystem);
			java.net.URI uri = new java.net.URI("har://" + harPath.ToString());
			harFileSystem.initialize(uri, conf);
			return harFileSystem;
		}

		/// <exception cref="System.Exception"/>
		private org.apache.hadoop.fs.HarFileSystem createHarFileSystem(org.apache.hadoop.conf.Configuration
			 conf, org.apache.hadoop.fs.Path aHarPath)
		{
			localFileSystem.mkdirs(aHarPath);
			org.apache.hadoop.fs.Path indexPath = new org.apache.hadoop.fs.Path(aHarPath, "_index"
				);
			org.apache.hadoop.fs.Path masterIndexPath = new org.apache.hadoop.fs.Path(aHarPath
				, "_masterindex");
			localFileSystem.createNewFile(indexPath);
			NUnit.Framework.Assert.IsTrue(localFileSystem.exists(indexPath));
			localFileSystem.createNewFile(masterIndexPath);
			NUnit.Framework.Assert.IsTrue(localFileSystem.exists(masterIndexPath));
			writeVersionToMasterIndexImpl(org.apache.hadoop.fs.HarFileSystem.VERSION, masterIndexPath
				);
			org.apache.hadoop.fs.HarFileSystem harFileSystem = new org.apache.hadoop.fs.HarFileSystem
				(localFileSystem);
			java.net.URI uri = new java.net.URI("har://" + aHarPath.ToString());
			harFileSystem.initialize(uri, conf);
			return harFileSystem;
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeVersionToMasterIndexImpl(int version, org.apache.hadoop.fs.Path
			 masterIndexPath)
		{
			// write Har version into the master index:
			org.apache.hadoop.fs.FSDataOutputStream fsdos = localFileSystem.create(masterIndexPath
				);
			try
			{
				string versionString = version + "\n";
				fsdos.write(Sharpen.Runtime.getBytesForString(versionString, "UTF-8"));
				fsdos.flush();
			}
			finally
			{
				fsdos.close();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void before()
		{
			java.io.File rootDirIoFile = new java.io.File(rootPath.toUri().getPath());
			rootDirIoFile.mkdirs();
			if (!rootDirIoFile.exists())
			{
				throw new System.IO.IOException("Failed to create temp directory [" + rootDirIoFile
					.getAbsolutePath() + "]");
			}
			// create Har to test:
			conf = new org.apache.hadoop.conf.Configuration();
			harFileSystem = createHarFileSystem(conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void after()
		{
			// close Har FS:
			org.apache.hadoop.fs.FileSystem harFS = harFileSystem;
			if (harFS != null)
			{
				harFS.close();
				harFileSystem = null;
			}
			// cleanup: delete all the temporary files:
			java.io.File rootDirIoFile = new java.io.File(rootPath.toUri().getPath());
			if (rootDirIoFile.exists())
			{
				org.apache.hadoop.fs.FileUtil.fullyDelete(rootDirIoFile);
			}
			if (rootDirIoFile.exists())
			{
				throw new System.IO.IOException("Failed to delete temp directory [" + rootDirIoFile
					.getAbsolutePath() + "]");
			}
		}

		// ======== Positive tests:
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testPositiveHarFileSystemBasics()
		{
			// check Har version:
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.HarFileSystem.VERSION, harFileSystem
				.getHarVersion());
			// check Har URI:
			java.net.URI harUri = harFileSystem.getUri();
			NUnit.Framework.Assert.AreEqual(harPath.toUri().getPath(), harUri.getPath());
			NUnit.Framework.Assert.AreEqual("har", harUri.getScheme());
			// check Har home path:
			org.apache.hadoop.fs.Path homePath = harFileSystem.getHomeDirectory();
			NUnit.Framework.Assert.AreEqual(harPath.toUri().getPath(), homePath.toUri().getPath
				());
			// check working directory:
			org.apache.hadoop.fs.Path workDirPath0 = harFileSystem.getWorkingDirectory();
			NUnit.Framework.Assert.AreEqual(homePath, workDirPath0);
			// check that its impossible to reset the working directory
			// (#setWorkingDirectory should have no effect):
			harFileSystem.setWorkingDirectory(new org.apache.hadoop.fs.Path("/foo/bar"));
			NUnit.Framework.Assert.AreEqual(workDirPath0, harFileSystem.getWorkingDirectory()
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testPositiveNewHarFsOnTheSameUnderlyingFs()
		{
			// Init 2nd har file system on the same underlying FS, so the
			// metadata gets reused:
			org.apache.hadoop.fs.HarFileSystem hfs = new org.apache.hadoop.fs.HarFileSystem(localFileSystem
				);
			java.net.URI uri = new java.net.URI("har://" + harPath.ToString());
			hfs.initialize(uri, new org.apache.hadoop.conf.Configuration());
			// the metadata should be reused from cache:
			NUnit.Framework.Assert.IsTrue(hfs.getMetadata() == harFileSystem.getMetadata());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testPositiveLruMetadataCacheFs()
		{
			// Init 2nd har file system on the same underlying FS, so the
			// metadata gets reused:
			org.apache.hadoop.fs.HarFileSystem hfs = new org.apache.hadoop.fs.HarFileSystem(localFileSystem
				);
			java.net.URI uri = new java.net.URI("har://" + harPath.ToString());
			hfs.initialize(uri, new org.apache.hadoop.conf.Configuration());
			// the metadata should be reused from cache:
			NUnit.Framework.Assert.IsTrue(hfs.getMetadata() == harFileSystem.getMetadata());
			// Create more hars, until the cache is full + 1; the last creation should evict the first entry from the cache
			for (int i = 0; i <= hfs.METADATA_CACHE_ENTRIES_DEFAULT; i++)
			{
				org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(rootPath, "path1/path2/my"
					 + i + ".har");
				createHarFileSystem(conf, p);
			}
			// The first entry should not be in the cache anymore:
			hfs = new org.apache.hadoop.fs.HarFileSystem(localFileSystem);
			uri = new java.net.URI("har://" + harPath.ToString());
			hfs.initialize(uri, new org.apache.hadoop.conf.Configuration());
			NUnit.Framework.Assert.IsTrue(hfs.getMetadata() != harFileSystem.getMetadata());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testPositiveInitWithoutUnderlyingFS()
		{
			// Init HarFS with no constructor arg, so that the underlying FS object
			// is created on demand or got from cache in #initialize() method.
			org.apache.hadoop.fs.HarFileSystem hfs = new org.apache.hadoop.fs.HarFileSystem();
			java.net.URI uri = new java.net.URI("har://" + harPath.ToString());
			hfs.initialize(uri, new org.apache.hadoop.conf.Configuration());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testPositiveListFilesNotEndInColon()
		{
			// re-initialize the har file system with host name
			// make sure the qualified path name does not append ":" at the end of host name
			java.net.URI uri = new java.net.URI("har://file-localhost" + harPath.ToString());
			harFileSystem.initialize(uri, conf);
			org.apache.hadoop.fs.Path p1 = new org.apache.hadoop.fs.Path("har://file-localhost"
				 + harPath.ToString());
			org.apache.hadoop.fs.Path p2 = harFileSystem.makeQualified(p1);
			NUnit.Framework.Assert.IsTrue(p2.toUri().ToString().StartsWith("har://file-localhost/"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testListLocatedStatus()
		{
			string testHarPath = Sharpen.Runtime.getClassForObject(this).getResource("/test.har"
				).getPath();
			java.net.URI uri = new java.net.URI("har://" + testHarPath);
			org.apache.hadoop.fs.HarFileSystem hfs = new org.apache.hadoop.fs.HarFileSystem(localFileSystem
				);
			hfs.initialize(uri, new org.apache.hadoop.conf.Configuration());
			// test.har has the following contents:
			//   dir1/1.txt
			//   dir1/2.txt
			System.Collections.Generic.ICollection<string> expectedFileNames = new java.util.HashSet
				<string>();
			expectedFileNames.add("1.txt");
			expectedFileNames.add("2.txt");
			// List contents of dir, and ensure we find all expected files
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("dir1");
			org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> fileList
				 = hfs.listLocatedStatus(path);
			while (fileList.hasNext())
			{
				string fileName = fileList.next().getPath().getName();
				NUnit.Framework.Assert.IsTrue(fileName + " not in expected files list", expectedFileNames
					.contains(fileName));
				expectedFileNames.remove(fileName);
			}
			NUnit.Framework.Assert.AreEqual("Didn't find all of the expected file names: " + 
				expectedFileNames, 0, expectedFileNames.Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMakeQualifiedPath()
		{
			// Construct a valid har file system path with authority that
			// contains userinfo and port. The userinfo and port are useless
			// in local fs uri. They are only used to verify har file system
			// can correctly preserve the information for the underlying file system.
			string harPathWithUserinfo = "har://file-user:passwd@localhost:80" + harPath.toUri
				().getPath().ToString();
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(harPathWithUserinfo
				);
			org.apache.hadoop.fs.Path qualifiedPath = path.getFileSystem(conf).makeQualified(
				path);
			NUnit.Framework.Assert.IsTrue(string.format("The qualified path (%s) did not match the expected path (%s)."
				, qualifiedPath.ToString(), harPathWithUserinfo), qualifiedPath.ToString().Equals
				(harPathWithUserinfo));
		}

		// ========== Negative:
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testNegativeInitWithoutIndex()
		{
			// delete the index file:
			org.apache.hadoop.fs.Path indexPath = new org.apache.hadoop.fs.Path(harPath, "_index"
				);
			localFileSystem.delete(indexPath, false);
			// now init the HarFs:
			org.apache.hadoop.fs.HarFileSystem hfs = new org.apache.hadoop.fs.HarFileSystem(localFileSystem
				);
			java.net.URI uri = new java.net.URI("har://" + harPath.ToString());
			try
			{
				hfs.initialize(uri, new org.apache.hadoop.conf.Configuration());
				NUnit.Framework.Assert.Fail("Exception expected.");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// ok, expected.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testNegativeGetHarVersionOnNotInitializedFS()
		{
			org.apache.hadoop.fs.HarFileSystem hfs = new org.apache.hadoop.fs.HarFileSystem(localFileSystem
				);
			try
			{
				int version = hfs.getHarVersion();
				NUnit.Framework.Assert.Fail("Exception expected, but got a Har version " + version
					 + ".");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// ok, expected.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testNegativeInitWithAnUnsupportedVersion()
		{
			// NB: should wait at least 1 second to ensure the timestamp of the master
			// index will change upon the writing, because Linux seems to update the
			// file modification
			// time with 1 second accuracy:
			java.lang.Thread.sleep(1000);
			// write an unsupported version:
			writeVersionToMasterIndexImpl(7777, new org.apache.hadoop.fs.Path(harPath, "_masterindex"
				));
			// init the Har:
			org.apache.hadoop.fs.HarFileSystem hfs = new org.apache.hadoop.fs.HarFileSystem(localFileSystem
				);
			// the metadata should *not* be reused from cache:
			NUnit.Framework.Assert.IsFalse(hfs.getMetadata() == harFileSystem.getMetadata());
			java.net.URI uri = new java.net.URI("har://" + harPath.ToString());
			try
			{
				hfs.initialize(uri, new org.apache.hadoop.conf.Configuration());
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// ok, expected.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testNegativeHarFsModifications()
		{
			// all the modification methods of HarFS must lead to IOE.
			org.apache.hadoop.fs.Path fooPath = new org.apache.hadoop.fs.Path(rootPath, "foo/bar"
				);
			localFileSystem.createNewFile(fooPath);
			try
			{
				harFileSystem.create(fooPath, new org.apache.hadoop.fs.permission.FsPermission("+rwx"
					), true, 1024, (short)88, 1024, null);
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (System.IO.IOException)
			{
			}
			// ok, expected.
			try
			{
				harFileSystem.setReplication(fooPath, (short)55);
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (System.IO.IOException)
			{
			}
			// ok, expected.
			try
			{
				harFileSystem.delete(fooPath, true);
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (System.IO.IOException)
			{
			}
			// ok, expected.
			try
			{
				harFileSystem.mkdirs(fooPath, new org.apache.hadoop.fs.permission.FsPermission("+rwx"
					));
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (System.IO.IOException)
			{
			}
			// ok, expected.
			org.apache.hadoop.fs.Path indexPath = new org.apache.hadoop.fs.Path(harPath, "_index"
				);
			try
			{
				harFileSystem.copyFromLocalFile(false, indexPath, fooPath);
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (System.IO.IOException)
			{
			}
			// ok, expected.
			try
			{
				harFileSystem.startLocalOutput(fooPath, indexPath);
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (System.IO.IOException)
			{
			}
			// ok, expected.
			try
			{
				harFileSystem.completeLocalOutput(fooPath, indexPath);
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (System.IO.IOException)
			{
			}
			// ok, expected.
			try
			{
				harFileSystem.setOwner(fooPath, "user", "group");
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (System.IO.IOException)
			{
			}
			// ok, expected.
			try
			{
				harFileSystem.setPermission(fooPath, new org.apache.hadoop.fs.permission.FsPermission
					("+x"));
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// ok, expected.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testHarFsWithoutAuthority()
		{
			java.net.URI uri = harFileSystem.getUri();
			NUnit.Framework.Assert.IsNull("har uri authority not null: " + uri, uri.getAuthority
				());
			org.apache.hadoop.fs.FileContext.getFileContext(uri, conf);
		}
	}
}
