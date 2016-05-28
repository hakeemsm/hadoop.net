using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Test that BlockPoolSliceStorage can correctly generate trash and
	/// restore directories for a given block file path.
	/// </summary>
	public class TestBlockPoolSliceStorage
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestBlockPoolSliceStorage
			));

		internal readonly Random rand = new Random();

		internal BlockPoolSliceStorage storage;

		/// <summary>BlockPoolSliceStorage with a dummy storage directory.</summary>
		/// <remarks>
		/// BlockPoolSliceStorage with a dummy storage directory. The directory
		/// need not exist. We need to extend BlockPoolSliceStorage so we can
		/// call
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.Storage.AddStorageDir(Org.Apache.Hadoop.Hdfs.Server.Common.Storage.StorageDirectory)
		/// 	"/>
		/// .
		/// </remarks>
		private class StubBlockPoolSliceStorage : BlockPoolSliceStorage
		{
			internal StubBlockPoolSliceStorage(int namespaceID, string bpID, long cTime, string
				 clusterId)
				: base(namespaceID, bpID, cTime, clusterId)
			{
				AddStorageDir(new Storage.StorageDirectory(new FilePath("/tmp/dontcare/" + bpID))
					);
				Assert.AssertThat(storageDirs.Count, IS.Is(1));
			}
		}

		private string MakeRandomIpAddress()
		{
			return rand.Next(256) + "." + rand.Next(256) + "." + rand.Next(256) + "." + rand.
				Next(256);
		}

		private string MakeRandomBlockpoolId()
		{
			return "BP-" + rand.Next(int.MaxValue) + "-" + MakeRandomIpAddress() + "-" + rand
				.Next(int.MaxValue);
		}

		private BlockPoolSliceStorage MakeBlockPoolStorage()
		{
			return new TestBlockPoolSliceStorage.StubBlockPoolSliceStorage(rand.Next(int.MaxValue
				), MakeRandomBlockpoolId(), rand.Next(int.MaxValue), UUID.RandomUUID().ToString(
				));
		}

		private string MakeRandomBlockFileSubdir(int nestingLevel)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(FilePath.separator);
			for (int i = 0; i < nestingLevel; ++i)
			{
				sb.Append("subdir" + rand.Next(64) + FilePath.separator);
			}
			return sb.ToString();
		}

		/// <summary>
		/// Test conversion from a block file path to its target trash
		/// directory.
		/// </summary>
		public virtual void GetTrashDirectoryForBlockFile(string fileName, int nestingLevel
			)
		{
			string blockFileSubdir = MakeRandomBlockFileSubdir(nestingLevel);
			string blockFileName = fileName;
			string testFilePath = storage.GetSingularStorageDir().GetRoot() + FilePath.separator
				 + Storage.StorageDirCurrent + blockFileSubdir + blockFileName;
			string expectedTrashPath = storage.GetSingularStorageDir().GetRoot() + FilePath.separator
				 + BlockPoolSliceStorage.TrashRootDir + Sharpen.Runtime.Substring(blockFileSubdir
				, 0, blockFileSubdir.Length - 1);
			Log.Info("Got subdir " + blockFileSubdir);
			Log.Info("Generated file path " + testFilePath);
			Assert.AssertThat(storage.GetTrashDirectory(new FilePath(testFilePath)), IS.Is(expectedTrashPath
				));
		}

		/*
		* Test conversion from a block file in a trash directory to its
		* target directory for restore.
		*/
		public virtual void GetRestoreDirectoryForBlockFile(string fileName, int nestingLevel
			)
		{
			BlockPoolSliceStorage storage = MakeBlockPoolStorage();
			string blockFileSubdir = MakeRandomBlockFileSubdir(nestingLevel);
			string blockFileName = fileName;
			string deletedFilePath = storage.GetSingularStorageDir().GetRoot() + FilePath.separator
				 + BlockPoolSliceStorage.TrashRootDir + blockFileSubdir + blockFileName;
			string expectedRestorePath = storage.GetSingularStorageDir().GetRoot() + FilePath
				.separator + Storage.StorageDirCurrent + Sharpen.Runtime.Substring(blockFileSubdir
				, 0, blockFileSubdir.Length - 1);
			Log.Info("Generated deleted file path " + deletedFilePath);
			Assert.AssertThat(storage.GetRestoreDirectory(new FilePath(deletedFilePath)), IS.Is
				(expectedRestorePath));
		}

		public virtual void TestGetTrashAndRestoreDirectories()
		{
			storage = MakeBlockPoolStorage();
			// Test a few different nesting levels since block files
			// could be nested such as subdir1/subdir5/blk_...
			// Make sure all nesting levels are handled correctly.
			for (int i = 0; i < 3; ++i)
			{
				GetTrashDirectoryForBlockFile("blk_myblockfile", i);
				GetTrashDirectoryForBlockFile("blk_myblockfile.meta", i);
				GetRestoreDirectoryForBlockFile("blk_myblockfile", i);
				GetRestoreDirectoryForBlockFile("blk_myblockfile.meta", i);
			}
		}
	}
}
