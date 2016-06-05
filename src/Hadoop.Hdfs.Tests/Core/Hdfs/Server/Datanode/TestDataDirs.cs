using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Hamcrest;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class TestDataDirs
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestDataDirParsing()
		{
			Configuration conf = new Configuration();
			IList<StorageLocation> locations;
			FilePath dir0 = new FilePath("/dir0");
			FilePath dir1 = new FilePath("/dir1");
			FilePath dir2 = new FilePath("/dir2");
			FilePath dir3 = new FilePath("/dir3");
			FilePath dir4 = new FilePath("/dir4");
			// Verify that a valid string is correctly parsed, and that storage
			// type is not case-sensitive
			string locations1 = "[disk]/dir0,[DISK]/dir1,[sSd]/dir2,[disK]/dir3,[ram_disk]/dir4";
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, locations1);
			locations = DataNode.GetStorageLocations(conf);
			Assert.AssertThat(locations.Count, CoreMatchers.Is(5));
			Assert.AssertThat(locations[0].GetStorageType(), CoreMatchers.Is(StorageType.Disk
				));
			Assert.AssertThat(locations[0].GetUri(), CoreMatchers.Is(dir0.ToURI()));
			Assert.AssertThat(locations[1].GetStorageType(), CoreMatchers.Is(StorageType.Disk
				));
			Assert.AssertThat(locations[1].GetUri(), CoreMatchers.Is(dir1.ToURI()));
			Assert.AssertThat(locations[2].GetStorageType(), CoreMatchers.Is(StorageType.Ssd)
				);
			Assert.AssertThat(locations[2].GetUri(), CoreMatchers.Is(dir2.ToURI()));
			Assert.AssertThat(locations[3].GetStorageType(), CoreMatchers.Is(StorageType.Disk
				));
			Assert.AssertThat(locations[3].GetUri(), CoreMatchers.Is(dir3.ToURI()));
			Assert.AssertThat(locations[4].GetStorageType(), CoreMatchers.Is(StorageType.RamDisk
				));
			Assert.AssertThat(locations[4].GetUri(), CoreMatchers.Is(dir4.ToURI()));
			// Verify that an unrecognized storage type result in an exception.
			string locations2 = "[BadMediaType]/dir0,[ssd]/dir1,[disk]/dir2";
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, locations2);
			try
			{
				locations = DataNode.GetStorageLocations(conf);
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException iae)
			{
				DataNode.Log.Info("The exception is expected.", iae);
			}
			// Assert that a string with no storage type specified is
			// correctly parsed and the default storage type is picked up.
			string locations3 = "/dir0,/dir1";
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, locations3);
			locations = DataNode.GetStorageLocations(conf);
			Assert.AssertThat(locations.Count, CoreMatchers.Is(2));
			Assert.AssertThat(locations[0].GetStorageType(), CoreMatchers.Is(StorageType.Disk
				));
			Assert.AssertThat(locations[0].GetUri(), CoreMatchers.Is(dir0.ToURI()));
			Assert.AssertThat(locations[1].GetStorageType(), CoreMatchers.Is(StorageType.Disk
				));
			Assert.AssertThat(locations[1].GetUri(), CoreMatchers.Is(dir1.ToURI()));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDataDirValidation()
		{
			DataNode.DataNodeDiskChecker diskChecker = Org.Mockito.Mockito.Mock<DataNode.DataNodeDiskChecker
				>();
			Org.Mockito.Mockito.DoThrow(new IOException()).DoThrow(new IOException()).DoNothing
				().When(diskChecker).CheckDir(Any<LocalFileSystem>(), Any<Path>());
			LocalFileSystem fs = Org.Mockito.Mockito.Mock<LocalFileSystem>();
			AbstractList<StorageLocation> locations = new AList<StorageLocation>();
			locations.AddItem(StorageLocation.Parse("file:/p1/"));
			locations.AddItem(StorageLocation.Parse("file:/p2/"));
			locations.AddItem(StorageLocation.Parse("file:/p3/"));
			IList<StorageLocation> checkedLocations = DataNode.CheckStorageLocations(locations
				, fs, diskChecker);
			NUnit.Framework.Assert.AreEqual("number of valid data dirs", 1, checkedLocations.
				Count);
			string validDir = checkedLocations.GetEnumerator().Next().GetFile().GetPath();
			Assert.AssertThat("p3 should be valid", new FilePath("/p3/").GetPath(), CoreMatchers.Is
				(validDir));
		}
	}
}
