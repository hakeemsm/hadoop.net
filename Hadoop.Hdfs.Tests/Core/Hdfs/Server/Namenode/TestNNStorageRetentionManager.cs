using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestNNStorageRetentionManager
	{
		internal readonly Configuration conf = new Configuration();

		/// <summary>
		/// For the purpose of this test, purge as many edits as we can
		/// with no extra "safety cushion"
		/// </summary>
		[SetUp]
		public virtual void SetNoExtraEditRetention()
		{
			conf.SetLong(DFSConfigKeys.DfsNamenodeNumExtraEditsRetainedKey, 0);
		}

		/// <summary>
		/// Test the "easy case" where we have more images in the
		/// directory than we need to keep.
		/// </summary>
		/// <remarks>
		/// Test the "easy case" where we have more images in the
		/// directory than we need to keep. Should purge the
		/// old ones.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPurgeEasyCase()
		{
			TestNNStorageRetentionManager.TestCaseDescription tc = new TestNNStorageRetentionManager.TestCaseDescription
				(this);
			tc.AddRoot("/foo1", NNStorage.NameNodeDirType.ImageAndEdits);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(100), true);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(200), true);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(300), false);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(400), false);
			tc.AddLog("/foo1/current/" + NNStorage.GetFinalizedEditsFileName(101, 200), true);
			tc.AddLog("/foo1/current/" + NNStorage.GetFinalizedEditsFileName(201, 300), true);
			tc.AddLog("/foo1/current/" + NNStorage.GetFinalizedEditsFileName(301, 400), false
				);
			tc.AddLog("/foo1/current/" + NNStorage.GetInProgressEditsFileName(401), false);
			// Test that other files don't get purged
			tc.AddLog("/foo1/current/VERSION", false);
			RunTest(tc);
		}

		/// <summary>Same as above, but across multiple directories</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPurgeMultipleDirs()
		{
			TestNNStorageRetentionManager.TestCaseDescription tc = new TestNNStorageRetentionManager.TestCaseDescription
				(this);
			tc.AddRoot("/foo1", NNStorage.NameNodeDirType.ImageAndEdits);
			tc.AddRoot("/foo2", NNStorage.NameNodeDirType.ImageAndEdits);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(100), true);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(200), true);
			tc.AddImage("/foo2/current/" + NNStorage.GetImageFileName(200), true);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(300), false);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(400), false);
			tc.AddLog("/foo1/current/" + NNStorage.GetFinalizedEditsFileName(101, 200), true);
			tc.AddLog("/foo1/current/" + NNStorage.GetFinalizedEditsFileName(201, 300), true);
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(201, 300), true);
			tc.AddLog("/foo1/current/" + NNStorage.GetFinalizedEditsFileName(301, 400), false
				);
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(301, 400), false
				);
			tc.AddLog("/foo1/current/" + NNStorage.GetInProgressEditsFileName(401), false);
			RunTest(tc);
		}

		/// <summary>
		/// Test that if we have fewer fsimages than the configured
		/// retention, we don't purge any of them
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPurgeLessThanRetention()
		{
			TestNNStorageRetentionManager.TestCaseDescription tc = new TestNNStorageRetentionManager.TestCaseDescription
				(this);
			tc.AddRoot("/foo1", NNStorage.NameNodeDirType.ImageAndEdits);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(100), false);
			tc.AddLog("/foo1/current/" + NNStorage.GetFinalizedEditsFileName(101, 200), false
				);
			tc.AddLog("/foo1/current/" + NNStorage.GetFinalizedEditsFileName(201, 300), false
				);
			tc.AddLog("/foo1/current/" + NNStorage.GetFinalizedEditsFileName(301, 400), false
				);
			tc.AddLog("/foo1/current/" + NNStorage.GetInProgressEditsFileName(401), false);
			RunTest(tc);
		}

		/// <summary>Check for edge case with no logs present at all.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNoLogs()
		{
			TestNNStorageRetentionManager.TestCaseDescription tc = new TestNNStorageRetentionManager.TestCaseDescription
				(this);
			tc.AddRoot("/foo1", NNStorage.NameNodeDirType.ImageAndEdits);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(100), true);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(200), true);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(300), false);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(400), false);
			RunTest(tc);
		}

		/// <summary>Check for edge case with no logs or images present at all.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEmptyDir()
		{
			TestNNStorageRetentionManager.TestCaseDescription tc = new TestNNStorageRetentionManager.TestCaseDescription
				(this);
			tc.AddRoot("/foo1", NNStorage.NameNodeDirType.ImageAndEdits);
			RunTest(tc);
		}

		/// <summary>Test that old in-progress logs are properly purged</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestOldInProgress()
		{
			TestNNStorageRetentionManager.TestCaseDescription tc = new TestNNStorageRetentionManager.TestCaseDescription
				(this);
			tc.AddRoot("/foo1", NNStorage.NameNodeDirType.ImageAndEdits);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(100), true);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(200), true);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(300), false);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(400), false);
			tc.AddLog("/foo1/current/" + NNStorage.GetInProgressEditsFileName(101), true);
			RunTest(tc);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSeparateEditDirs()
		{
			TestNNStorageRetentionManager.TestCaseDescription tc = new TestNNStorageRetentionManager.TestCaseDescription
				(this);
			tc.AddRoot("/foo1", NNStorage.NameNodeDirType.Image);
			tc.AddRoot("/foo2", NNStorage.NameNodeDirType.Edits);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(100), true);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(200), true);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(300), false);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(400), false);
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(101, 200), true);
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(201, 300), true);
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(301, 400), false
				);
			tc.AddLog("/foo2/current/" + NNStorage.GetInProgressEditsFileName(401), false);
			RunTest(tc);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRetainExtraLogs()
		{
			conf.SetLong(DFSConfigKeys.DfsNamenodeNumExtraEditsRetainedKey, 50);
			TestNNStorageRetentionManager.TestCaseDescription tc = new TestNNStorageRetentionManager.TestCaseDescription
				(this);
			tc.AddRoot("/foo1", NNStorage.NameNodeDirType.Image);
			tc.AddRoot("/foo2", NNStorage.NameNodeDirType.Edits);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(100), true);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(200), true);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(300), false);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(400), false);
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(101, 200), true);
			// Since we need 50 extra edits, *do* retain the 201-300 segment 
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(201, 300), false
				);
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(301, 400), false
				);
			tc.AddLog("/foo2/current/" + NNStorage.GetInProgressEditsFileName(401), false);
			RunTest(tc);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRetainExtraLogsLimitedSegments()
		{
			conf.SetLong(DFSConfigKeys.DfsNamenodeNumExtraEditsRetainedKey, 150);
			conf.SetLong(DFSConfigKeys.DfsNamenodeMaxExtraEditsSegmentsRetainedKey, 2);
			TestNNStorageRetentionManager.TestCaseDescription tc = new TestNNStorageRetentionManager.TestCaseDescription
				(this);
			tc.AddRoot("/foo1", NNStorage.NameNodeDirType.Image);
			tc.AddRoot("/foo2", NNStorage.NameNodeDirType.Edits);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(100), true);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(200), true);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(300), false);
			tc.AddImage("/foo1/current/" + NNStorage.GetImageFileName(400), false);
			// Segments containing txns upto txId 250 are extra and should be purged.
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(1, 100), true);
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(101, 175), true);
			tc.AddLog("/foo2/current/" + NNStorage.GetInProgressEditsFileName(176) + ".empty"
				, true);
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(176, 200), true);
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(201, 225), true);
			tc.AddLog("/foo2/current/" + NNStorage.GetInProgressEditsFileName(226) + ".corrupt"
				, true);
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(226, 240), true);
			// Only retain 2 extra segments. The 301-350 and 351-400 segments are
			// considered required, not extra.
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(241, 275), false
				);
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(276, 300), false
				);
			tc.AddLog("/foo2/current/" + NNStorage.GetInProgressEditsFileName(301) + ".empty"
				, false);
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(301, 350), false
				);
			tc.AddLog("/foo2/current/" + NNStorage.GetInProgressEditsFileName(351) + ".corrupt"
				, false);
			tc.AddLog("/foo2/current/" + NNStorage.GetFinalizedEditsFileName(351, 400), false
				);
			tc.AddLog("/foo2/current/" + NNStorage.GetInProgressEditsFileName(401), false);
			RunTest(tc);
		}

		/// <exception cref="System.IO.IOException"/>
		private void RunTest(TestNNStorageRetentionManager.TestCaseDescription tc)
		{
			NNStorageRetentionManager.StoragePurger mockPurger = Org.Mockito.Mockito.Mock<NNStorageRetentionManager.StoragePurger
				>();
			ArgumentCaptor<FSImageStorageInspector.FSImageFile> imagesPurgedCaptor = ArgumentCaptor
				.ForClass<FSImageStorageInspector.FSImageFile>();
			ArgumentCaptor<FileJournalManager.EditLogFile> logsPurgedCaptor = ArgumentCaptor.
				ForClass<FileJournalManager.EditLogFile>();
			// Ask the manager to purge files we don't need any more
			new NNStorageRetentionManager(conf, tc.MockStorage(), tc.MockEditLog(mockPurger), 
				mockPurger).PurgeOldStorage(NNStorage.NameNodeFile.Image);
			// Verify that it asked the purger to remove the correct files
			Org.Mockito.Mockito.Verify(mockPurger, Org.Mockito.Mockito.AtLeast(0)).PurgeImage
				(imagesPurgedCaptor.Capture());
			Org.Mockito.Mockito.Verify(mockPurger, Org.Mockito.Mockito.AtLeast(0)).PurgeLog(logsPurgedCaptor
				.Capture());
			// Check images
			ICollection<string> purgedPaths = Sets.NewLinkedHashSet();
			foreach (FSImageStorageInspector.FSImageFile purged in imagesPurgedCaptor.GetAllValues
				())
			{
				purgedPaths.AddItem(FileToPath(purged.GetFile()));
			}
			NUnit.Framework.Assert.AreEqual(Joiner.On(",").Join(FilesToPaths(tc.expectedPurgedImages
				)), Joiner.On(",").Join(purgedPaths));
			// Check images
			purgedPaths.Clear();
			foreach (FileJournalManager.EditLogFile purged_1 in logsPurgedCaptor.GetAllValues
				())
			{
				purgedPaths.AddItem(FileToPath(purged_1.GetFile()));
			}
			NUnit.Framework.Assert.AreEqual(Joiner.On(",").Join(FilesToPaths(tc.expectedPurgedLogs
				)), Joiner.On(",").Join(purgedPaths));
		}

		private class TestCaseDescription
		{
			private readonly IDictionary<FilePath, TestNNStorageRetentionManager.TestCaseDescription.FakeRoot
				> dirRoots = Maps.NewLinkedHashMap();

			private readonly ICollection<FilePath> expectedPurgedLogs = Sets.NewLinkedHashSet
				();

			private readonly ICollection<FilePath> expectedPurgedImages = Sets.NewLinkedHashSet
				();

			private class FakeRoot
			{
				internal readonly NNStorage.NameNodeDirType type;

				internal readonly IList<FilePath> files;

				internal FakeRoot(TestCaseDescription _enclosing, NNStorage.NameNodeDirType type)
				{
					this._enclosing = _enclosing;
					this.type = type;
					this.files = Lists.NewArrayList();
				}

				internal virtual Storage.StorageDirectory MockStorageDir()
				{
					return FSImageTestUtil.MockStorageDirectory(this.type, false, Sharpen.Collections.ToArray
						(TestNNStorageRetentionManager.FilesToPaths(this.files), new string[0]));
				}

				private readonly TestCaseDescription _enclosing;
			}

			internal virtual void AddRoot(string root, NNStorage.NameNodeDirType dir)
			{
				this.dirRoots[new FilePath(root)] = new TestNNStorageRetentionManager.TestCaseDescription.FakeRoot
					(this, dir);
			}

			private void AddFile(FilePath file)
			{
				foreach (KeyValuePair<FilePath, TestNNStorageRetentionManager.TestCaseDescription.FakeRoot
					> entry in this.dirRoots)
				{
					if (TestNNStorageRetentionManager.FileToPath(file).StartsWith(TestNNStorageRetentionManager
						.FileToPath(entry.Key)))
					{
						entry.Value.files.AddItem(file);
					}
				}
			}

			internal virtual void AddLog(string path, bool expectPurge)
			{
				FilePath file = new FilePath(path);
				this.AddFile(file);
				if (expectPurge)
				{
					this.expectedPurgedLogs.AddItem(file);
				}
			}

			internal virtual void AddImage(string path, bool expectPurge)
			{
				FilePath file = new FilePath(path);
				this.AddFile(file);
				if (expectPurge)
				{
					this.expectedPurgedImages.AddItem(file);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual NNStorage MockStorage()
			{
				IList<Storage.StorageDirectory> sds = Lists.NewArrayList();
				foreach (TestNNStorageRetentionManager.TestCaseDescription.FakeRoot root in this.
					dirRoots.Values)
				{
					sds.AddItem(root.MockStorageDir());
				}
				return TestNNStorageRetentionManager.MockStorageForDirs(Sharpen.Collections.ToArray
					(sds, new Storage.StorageDirectory[0]));
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FSEditLog MockEditLog(NNStorageRetentionManager.StoragePurger purger
				)
			{
				IList<JournalManager> jms = Lists.NewArrayList();
				JournalSet journalSet = new JournalSet(0);
				foreach (TestNNStorageRetentionManager.TestCaseDescription.FakeRoot root in this.
					dirRoots.Values)
				{
					if (!root.type.IsOfType(NNStorage.NameNodeDirType.Edits))
					{
						continue;
					}
					// passing null NNStorage for unit test because it does not use it
					FileJournalManager fjm = new FileJournalManager(this._enclosing.conf, root.MockStorageDir
						(), null);
					fjm.purger = purger;
					jms.AddItem(fjm);
					journalSet.Add(fjm, false);
				}
				FSEditLog mockLog = Org.Mockito.Mockito.Mock<FSEditLog>();
				Org.Mockito.Mockito.DoAnswer(new _Answer_350(jms)).When(mockLog).PurgeLogsOlderThan
					(Org.Mockito.Mockito.AnyLong());
				Org.Mockito.Mockito.DoAnswer(new _Answer_365(journalSet)).When(mockLog).SelectInputStreams
					(Org.Mockito.Mockito.AnyCollection(), Org.Mockito.Mockito.AnyLong(), Org.Mockito.Mockito
					.AnyBoolean());
				return mockLog;
			}

			private sealed class _Answer_350 : Answer<Void>
			{
				public _Answer_350(IList<JournalManager> jms)
				{
					this.jms = jms;
				}

				/// <exception cref="System.Exception"/>
				public Void Answer(InvocationOnMock invocation)
				{
					object[] args = invocation.GetArguments();
					System.Diagnostics.Debug.Assert(args.Length == 1);
					long txId = (long)args[0];
					foreach (JournalManager jm in jms)
					{
						jm.PurgeLogsOlderThan(txId);
					}
					return null;
				}

				private readonly IList<JournalManager> jms;
			}

			private sealed class _Answer_365 : Answer<Void>
			{
				public _Answer_365(JournalSet journalSet)
				{
					this.journalSet = journalSet;
				}

				/// <exception cref="System.Exception"/>
				public Void Answer(InvocationOnMock invocation)
				{
					object[] args = invocation.GetArguments();
					journalSet.SelectInputStreams((ICollection<EditLogInputStream>)args[0], (long)args
						[1], (bool)args[2]);
					return null;
				}

				private readonly JournalSet journalSet;
			}

			internal TestCaseDescription(TestNNStorageRetentionManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestNNStorageRetentionManager _enclosing;
		}

		/// <summary>Converts a file to a platform-agnostic URI path.</summary>
		/// <param name="file">File to convert</param>
		/// <returns>String path</returns>
		private static string FileToPath(FilePath file)
		{
			return file.ToURI().GetPath();
		}

		/// <summary>Converts multiple files to platform-agnostic URI paths.</summary>
		/// <param name="files">Collection<File> files to convert</param>
		/// <returns>Collection<String> paths</returns>
		private static ICollection<string> FilesToPaths(ICollection<FilePath> files)
		{
			IList<string> paths = Lists.NewArrayList();
			foreach (FilePath file in files)
			{
				paths.AddItem(FileToPath(file));
			}
			return paths;
		}

		/// <exception cref="System.IO.IOException"/>
		private static NNStorage MockStorageForDirs(params Storage.StorageDirectory[] mockDirs
			)
		{
			NNStorage mockStorage = Org.Mockito.Mockito.Mock<NNStorage>();
			Org.Mockito.Mockito.DoAnswer(new _Answer_407(mockDirs)).When(mockStorage).InspectStorageDirs
				(Org.Mockito.Mockito.AnyObject<FSImageStorageInspector>());
			return mockStorage;
		}

		private sealed class _Answer_407 : Answer<Void>
		{
			public _Answer_407(Storage.StorageDirectory[] mockDirs)
			{
				this.mockDirs = mockDirs;
			}

			/// <exception cref="System.Exception"/>
			public Void Answer(InvocationOnMock invocation)
			{
				FSImageStorageInspector inspector = (FSImageStorageInspector)invocation.GetArguments
					()[0];
				foreach (Storage.StorageDirectory sd in mockDirs)
				{
					inspector.InspectDirectory(sd);
				}
				return null;
			}

			private readonly Storage.StorageDirectory[] mockDirs;
		}
	}
}
