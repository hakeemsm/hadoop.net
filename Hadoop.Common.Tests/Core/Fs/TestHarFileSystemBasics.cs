using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS
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
		private static readonly string RootPath = Runtime.GetProperty("test.build.data", 
			"build/test/data");

		private static readonly Path rootPath;

		static TestHarFileSystemBasics()
		{
			string root = new Path(new FilePath(RootPath).GetAbsolutePath(), "localfs").ToUri
				().GetPath();
			// Strip drive specifier on Windows, which would make the HAR URI invalid and
			// cause tests to fail.
			if (Shell.Windows)
			{
				root = Runtime.Substring(root, root.IndexOf(':') + 1);
			}
			rootPath = new Path(root);
		}

		private static readonly Path harPath = new Path(rootPath, "path1/path2/my.har");

		private FileSystem localFileSystem;

		private HarFileSystem harFileSystem;

		private Configuration conf;

		// NB: .har suffix is necessary
		/*
		* creates and returns fully initialized HarFileSystem
		*/
		/// <exception cref="System.Exception"/>
		private HarFileSystem CreateHarFileSystem(Configuration conf)
		{
			localFileSystem = FileSystem.GetLocal(conf);
			localFileSystem.Initialize(new URI("file:///"), conf);
			localFileSystem.Mkdirs(rootPath);
			localFileSystem.Mkdirs(harPath);
			Path indexPath = new Path(harPath, "_index");
			Path masterIndexPath = new Path(harPath, "_masterindex");
			localFileSystem.CreateNewFile(indexPath);
			Assert.True(localFileSystem.Exists(indexPath));
			localFileSystem.CreateNewFile(masterIndexPath);
			Assert.True(localFileSystem.Exists(masterIndexPath));
			WriteVersionToMasterIndexImpl(HarFileSystem.Version, masterIndexPath);
			HarFileSystem harFileSystem = new HarFileSystem(localFileSystem);
			URI uri = new URI("har://" + harPath.ToString());
			harFileSystem.Initialize(uri, conf);
			return harFileSystem;
		}

		/// <exception cref="System.Exception"/>
		private HarFileSystem CreateHarFileSystem(Configuration conf, Path aHarPath)
		{
			localFileSystem.Mkdirs(aHarPath);
			Path indexPath = new Path(aHarPath, "_index");
			Path masterIndexPath = new Path(aHarPath, "_masterindex");
			localFileSystem.CreateNewFile(indexPath);
			Assert.True(localFileSystem.Exists(indexPath));
			localFileSystem.CreateNewFile(masterIndexPath);
			Assert.True(localFileSystem.Exists(masterIndexPath));
			WriteVersionToMasterIndexImpl(HarFileSystem.Version, masterIndexPath);
			HarFileSystem harFileSystem = new HarFileSystem(localFileSystem);
			URI uri = new URI("har://" + aHarPath.ToString());
			harFileSystem.Initialize(uri, conf);
			return harFileSystem;
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteVersionToMasterIndexImpl(int version, Path masterIndexPath)
		{
			// write Har version into the master index:
			FSDataOutputStream fsdos = localFileSystem.Create(masterIndexPath);
			try
			{
				string versionString = version + "\n";
				fsdos.Write(Runtime.GetBytesForString(versionString, "UTF-8"));
				fsdos.Flush();
			}
			finally
			{
				fsdos.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Before()
		{
			FilePath rootDirIoFile = new FilePath(rootPath.ToUri().GetPath());
			rootDirIoFile.Mkdirs();
			if (!rootDirIoFile.Exists())
			{
				throw new IOException("Failed to create temp directory [" + rootDirIoFile.GetAbsolutePath
					() + "]");
			}
			// create Har to test:
			conf = new Configuration();
			harFileSystem = CreateHarFileSystem(conf);
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void After()
		{
			// close Har FS:
			FileSystem harFS = harFileSystem;
			if (harFS != null)
			{
				harFS.Close();
				harFileSystem = null;
			}
			// cleanup: delete all the temporary files:
			FilePath rootDirIoFile = new FilePath(rootPath.ToUri().GetPath());
			if (rootDirIoFile.Exists())
			{
				FileUtil.FullyDelete(rootDirIoFile);
			}
			if (rootDirIoFile.Exists())
			{
				throw new IOException("Failed to delete temp directory [" + rootDirIoFile.GetAbsolutePath
					() + "]");
			}
		}

		// ======== Positive tests:
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestPositiveHarFileSystemBasics()
		{
			// check Har version:
			Assert.Equal(HarFileSystem.Version, harFileSystem.GetHarVersion
				());
			// check Har URI:
			URI harUri = harFileSystem.GetUri();
			Assert.Equal(harPath.ToUri().GetPath(), harUri.GetPath());
			Assert.Equal("har", harUri.GetScheme());
			// check Har home path:
			Path homePath = harFileSystem.GetHomeDirectory();
			Assert.Equal(harPath.ToUri().GetPath(), homePath.ToUri().GetPath
				());
			// check working directory:
			Path workDirPath0 = harFileSystem.GetWorkingDirectory();
			Assert.Equal(homePath, workDirPath0);
			// check that its impossible to reset the working directory
			// (#setWorkingDirectory should have no effect):
			harFileSystem.SetWorkingDirectory(new Path("/foo/bar"));
			Assert.Equal(workDirPath0, harFileSystem.GetWorkingDirectory()
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestPositiveNewHarFsOnTheSameUnderlyingFs()
		{
			// Init 2nd har file system on the same underlying FS, so the
			// metadata gets reused:
			HarFileSystem hfs = new HarFileSystem(localFileSystem);
			URI uri = new URI("har://" + harPath.ToString());
			hfs.Initialize(uri, new Configuration());
			// the metadata should be reused from cache:
			Assert.True(hfs.GetMetadata() == harFileSystem.GetMetadata());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestPositiveLruMetadataCacheFs()
		{
			// Init 2nd har file system on the same underlying FS, so the
			// metadata gets reused:
			HarFileSystem hfs = new HarFileSystem(localFileSystem);
			URI uri = new URI("har://" + harPath.ToString());
			hfs.Initialize(uri, new Configuration());
			// the metadata should be reused from cache:
			Assert.True(hfs.GetMetadata() == harFileSystem.GetMetadata());
			// Create more hars, until the cache is full + 1; the last creation should evict the first entry from the cache
			for (int i = 0; i <= hfs.MetadataCacheEntriesDefault; i++)
			{
				Path p = new Path(rootPath, "path1/path2/my" + i + ".har");
				CreateHarFileSystem(conf, p);
			}
			// The first entry should not be in the cache anymore:
			hfs = new HarFileSystem(localFileSystem);
			uri = new URI("har://" + harPath.ToString());
			hfs.Initialize(uri, new Configuration());
			Assert.True(hfs.GetMetadata() != harFileSystem.GetMetadata());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestPositiveInitWithoutUnderlyingFS()
		{
			// Init HarFS with no constructor arg, so that the underlying FS object
			// is created on demand or got from cache in #initialize() method.
			HarFileSystem hfs = new HarFileSystem();
			URI uri = new URI("har://" + harPath.ToString());
			hfs.Initialize(uri, new Configuration());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestPositiveListFilesNotEndInColon()
		{
			// re-initialize the har file system with host name
			// make sure the qualified path name does not append ":" at the end of host name
			URI uri = new URI("har://file-localhost" + harPath.ToString());
			harFileSystem.Initialize(uri, conf);
			Path p1 = new Path("har://file-localhost" + harPath.ToString());
			Path p2 = harFileSystem.MakeQualified(p1);
			Assert.True(p2.ToUri().ToString().StartsWith("har://file-localhost/"
				));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestListLocatedStatus()
		{
			string testHarPath = this.GetType().GetResource("/test.har").AbsolutePath;
			URI uri = new URI("har://" + testHarPath);
			HarFileSystem hfs = new HarFileSystem(localFileSystem);
			hfs.Initialize(uri, new Configuration());
			// test.har has the following contents:
			//   dir1/1.txt
			//   dir1/2.txt
			ICollection<string> expectedFileNames = new HashSet<string>();
			expectedFileNames.AddItem("1.txt");
			expectedFileNames.AddItem("2.txt");
			// List contents of dir, and ensure we find all expected files
			Path path = new Path("dir1");
			RemoteIterator<LocatedFileStatus> fileList = hfs.ListLocatedStatus(path);
			while (fileList.HasNext())
			{
				string fileName = fileList.Next().GetPath().GetName();
				Assert.True(fileName + " not in expected files list", expectedFileNames
					.Contains(fileName));
				expectedFileNames.Remove(fileName);
			}
			Assert.Equal("Didn't find all of the expected file names: " + 
				expectedFileNames, 0, expectedFileNames.Count);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMakeQualifiedPath()
		{
			// Construct a valid har file system path with authority that
			// contains userinfo and port. The userinfo and port are useless
			// in local fs uri. They are only used to verify har file system
			// can correctly preserve the information for the underlying file system.
			string harPathWithUserinfo = "har://file-user:passwd@localhost:80" + harPath.ToUri
				().GetPath().ToString();
			Path path = new Path(harPathWithUserinfo);
			Path qualifiedPath = path.GetFileSystem(conf).MakeQualified(path);
			Assert.True(string.Format("The qualified path (%s) did not match the expected path (%s)."
				, qualifiedPath.ToString(), harPathWithUserinfo), qualifiedPath.ToString().Equals
				(harPathWithUserinfo));
		}

		// ========== Negative:
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNegativeInitWithoutIndex()
		{
			// delete the index file:
			Path indexPath = new Path(harPath, "_index");
			localFileSystem.Delete(indexPath, false);
			// now init the HarFs:
			HarFileSystem hfs = new HarFileSystem(localFileSystem);
			URI uri = new URI("har://" + harPath.ToString());
			try
			{
				hfs.Initialize(uri, new Configuration());
				NUnit.Framework.Assert.Fail("Exception expected.");
			}
			catch (IOException)
			{
			}
		}

		// ok, expected.
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNegativeGetHarVersionOnNotInitializedFS()
		{
			HarFileSystem hfs = new HarFileSystem(localFileSystem);
			try
			{
				int version = hfs.GetHarVersion();
				NUnit.Framework.Assert.Fail("Exception expected, but got a Har version " + version
					 + ".");
			}
			catch (IOException)
			{
			}
		}

		// ok, expected.
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNegativeInitWithAnUnsupportedVersion()
		{
			// NB: should wait at least 1 second to ensure the timestamp of the master
			// index will change upon the writing, because Linux seems to update the
			// file modification
			// time with 1 second accuracy:
			Thread.Sleep(1000);
			// write an unsupported version:
			WriteVersionToMasterIndexImpl(7777, new Path(harPath, "_masterindex"));
			// init the Har:
			HarFileSystem hfs = new HarFileSystem(localFileSystem);
			// the metadata should *not* be reused from cache:
			NUnit.Framework.Assert.IsFalse(hfs.GetMetadata() == harFileSystem.GetMetadata());
			URI uri = new URI("har://" + harPath.ToString());
			try
			{
				hfs.Initialize(uri, new Configuration());
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (IOException)
			{
			}
		}

		// ok, expected.
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNegativeHarFsModifications()
		{
			// all the modification methods of HarFS must lead to IOE.
			Path fooPath = new Path(rootPath, "foo/bar");
			localFileSystem.CreateNewFile(fooPath);
			try
			{
				harFileSystem.Create(fooPath, new FsPermission("+rwx"), true, 1024, (short)88, 1024
					, null);
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (IOException)
			{
			}
			// ok, expected.
			try
			{
				harFileSystem.SetReplication(fooPath, (short)55);
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (IOException)
			{
			}
			// ok, expected.
			try
			{
				harFileSystem.Delete(fooPath, true);
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (IOException)
			{
			}
			// ok, expected.
			try
			{
				harFileSystem.Mkdirs(fooPath, new FsPermission("+rwx"));
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (IOException)
			{
			}
			// ok, expected.
			Path indexPath = new Path(harPath, "_index");
			try
			{
				harFileSystem.CopyFromLocalFile(false, indexPath, fooPath);
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (IOException)
			{
			}
			// ok, expected.
			try
			{
				harFileSystem.StartLocalOutput(fooPath, indexPath);
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (IOException)
			{
			}
			// ok, expected.
			try
			{
				harFileSystem.CompleteLocalOutput(fooPath, indexPath);
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (IOException)
			{
			}
			// ok, expected.
			try
			{
				harFileSystem.SetOwner(fooPath, "user", "group");
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (IOException)
			{
			}
			// ok, expected.
			try
			{
				harFileSystem.SetPermission(fooPath, new FsPermission("+x"));
				NUnit.Framework.Assert.Fail("IOException expected.");
			}
			catch (IOException)
			{
			}
		}

		// ok, expected.
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestHarFsWithoutAuthority()
		{
			URI uri = harFileSystem.GetUri();
			NUnit.Framework.Assert.IsNull("har uri authority not null: " + uri, uri.GetAuthority
				());
			FileContext.GetFileContext(uri, conf);
		}
	}
}
