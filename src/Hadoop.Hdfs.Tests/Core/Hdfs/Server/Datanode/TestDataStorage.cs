using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class TestDataStorage
	{
		private const string DefaultBpid = "bp-0";

		private const string ClusterId = "cluster0";

		private const string BuildVersion = "2.0";

		private const string SoftwareVersion = "2.0";

		private const long Ctime = 1;

		private static readonly FilePath TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			) + "/dstest");

		private static readonly HdfsServerConstants.StartupOption StartOpt = HdfsServerConstants.StartupOption
			.Regular;

		private DataNode mockDN = Org.Mockito.Mockito.Mock<DataNode>();

		private NamespaceInfo nsInfo;

		private DataStorage storage;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			storage = new DataStorage();
			nsInfo = new NamespaceInfo(0, ClusterId, DefaultBpid, Ctime, BuildVersion, SoftwareVersion
				);
			FileUtil.FullyDelete(TestDir);
			NUnit.Framework.Assert.IsTrue("Failed to make test dir.", TestDir.Mkdirs());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			storage.UnlockAll();
			FileUtil.FullyDelete(TestDir);
		}

		/// <exception cref="System.IO.IOException"/>
		private static IList<StorageLocation> CreateStorageLocations(int numLocs)
		{
			return CreateStorageLocations(numLocs, false);
		}

		/// <summary>Create a list of StorageLocations.</summary>
		/// <remarks>
		/// Create a list of StorageLocations.
		/// If asFile sets to true, create StorageLocation as regular files, otherwise
		/// create directories for each location.
		/// </remarks>
		/// <param name="numLocs">the total number of StorageLocations to be created.</param>
		/// <param name="asFile">set to true to create as file.</param>
		/// <returns>a list of StorageLocations.</returns>
		/// <exception cref="System.IO.IOException"/>
		private static IList<StorageLocation> CreateStorageLocations(int numLocs, bool asFile
			)
		{
			IList<StorageLocation> locations = new AList<StorageLocation>();
			for (int i = 0; i < numLocs; i++)
			{
				string uri = TestDir + "/data" + i;
				FilePath file = new FilePath(uri);
				if (asFile)
				{
					file.GetParentFile().Mkdirs();
					file.CreateNewFile();
				}
				else
				{
					file.Mkdirs();
				}
				StorageLocation loc = StorageLocation.Parse(uri);
				locations.AddItem(loc);
			}
			return locations;
		}

		private static IList<NamespaceInfo> CreateNamespaceInfos(int num)
		{
			IList<NamespaceInfo> nsInfos = new AList<NamespaceInfo>();
			for (int i = 0; i < num; i++)
			{
				string bpid = "bp-" + i;
				nsInfos.AddItem(new NamespaceInfo(0, ClusterId, bpid, Ctime, BuildVersion, SoftwareVersion
					));
			}
			return nsInfos;
		}

		/// <summary>Check whether the path is a valid DataNode data directory.</summary>
		private static void CheckDir(FilePath dataDir)
		{
			Storage.StorageDirectory sd = new Storage.StorageDirectory(dataDir);
			NUnit.Framework.Assert.IsTrue(sd.GetRoot().IsDirectory());
			NUnit.Framework.Assert.IsTrue(sd.GetCurrentDir().IsDirectory());
			NUnit.Framework.Assert.IsTrue(sd.GetVersionFile().IsFile());
		}

		/// <summary>Check whether the root is a valid BlockPoolSlice storage.</summary>
		private static void CheckDir(FilePath root, string bpid)
		{
			Storage.StorageDirectory sd = new Storage.StorageDirectory(root);
			FilePath bpRoot = new FilePath(sd.GetCurrentDir(), bpid);
			Storage.StorageDirectory bpSd = new Storage.StorageDirectory(bpRoot);
			NUnit.Framework.Assert.IsTrue(bpSd.GetRoot().IsDirectory());
			NUnit.Framework.Assert.IsTrue(bpSd.GetCurrentDir().IsDirectory());
			NUnit.Framework.Assert.IsTrue(bpSd.GetVersionFile().IsFile());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestAddStorageDirectories()
		{
			int numLocations = 3;
			int numNamespace = 3;
			IList<StorageLocation> locations = CreateStorageLocations(numLocations);
			// Add volumes for multiple namespaces.
			IList<NamespaceInfo> namespaceInfos = CreateNamespaceInfos(numNamespace);
			foreach (NamespaceInfo ni in namespaceInfos)
			{
				storage.AddStorageLocations(mockDN, ni, locations, StartOpt);
				foreach (StorageLocation sl in locations)
				{
					CheckDir(sl.GetFile());
					CheckDir(sl.GetFile(), ni.GetBlockPoolID());
				}
			}
			NUnit.Framework.Assert.AreEqual(numLocations, storage.GetNumStorageDirs());
			locations = CreateStorageLocations(numLocations);
			IList<StorageLocation> addedLocation = storage.AddStorageLocations(mockDN, namespaceInfos
				[0], locations, StartOpt);
			NUnit.Framework.Assert.IsTrue(addedLocation.IsEmpty());
			// The number of active storage dirs has not changed, since it tries to
			// add the storage dirs that are under service.
			NUnit.Framework.Assert.AreEqual(numLocations, storage.GetNumStorageDirs());
			// Add more directories.
			locations = CreateStorageLocations(6);
			storage.AddStorageLocations(mockDN, nsInfo, locations, StartOpt);
			NUnit.Framework.Assert.AreEqual(6, storage.GetNumStorageDirs());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRecoverTransitionReadFailure()
		{
			int numLocations = 3;
			IList<StorageLocation> locations = CreateStorageLocations(numLocations, true);
			try
			{
				storage.RecoverTransitionRead(mockDN, nsInfo, locations, StartOpt);
				NUnit.Framework.Assert.Fail("An IOException should throw: all StorageLocations are NON_EXISTENT"
					);
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("All specified directories are failed to load."
					, e);
			}
			NUnit.Framework.Assert.AreEqual(0, storage.GetNumStorageDirs());
		}

		/// <summary>
		/// This test enforces the behavior that if there is an exception from
		/// doTransition() during DN starts up, the storage directories that have
		/// already been processed are still visible, i.e., in
		/// DataStorage.storageDirs().
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRecoverTransitionReadDoTransitionFailure()
		{
			int numLocations = 3;
			IList<StorageLocation> locations = CreateStorageLocations(numLocations);
			// Prepare volumes
			storage.RecoverTransitionRead(mockDN, nsInfo, locations, StartOpt);
			NUnit.Framework.Assert.AreEqual(numLocations, storage.GetNumStorageDirs());
			// Reset DataStorage
			storage.UnlockAll();
			storage = new DataStorage();
			// Trigger an exception from doTransition().
			nsInfo.clusterID = "cluster1";
			try
			{
				storage.RecoverTransitionRead(mockDN, nsInfo, locations, StartOpt);
				NUnit.Framework.Assert.Fail("Expect to throw an exception from doTransition()");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("All specified directories", e);
			}
			NUnit.Framework.Assert.AreEqual(0, storage.GetNumStorageDirs());
		}
	}
}
