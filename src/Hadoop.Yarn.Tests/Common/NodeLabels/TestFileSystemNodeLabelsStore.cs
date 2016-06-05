using System.Collections;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Nodelabels
{
	public class TestFileSystemNodeLabelsStore : NodeLabelTestBase
	{
		internal TestFileSystemNodeLabelsStore.MockNodeLabelManager mgr = null;

		internal Configuration conf = null;

		private class MockNodeLabelManager : CommonNodeLabelsManager
		{
			protected internal override void InitDispatcher(Configuration conf)
			{
				base.dispatcher = new InlineDispatcher();
			}

			protected internal override void StartDispatcher()
			{
			}

			// do nothing
			protected internal override void StopDispatcher()
			{
			}
			// do nothing
		}

		private FileSystemNodeLabelsStore GetStore()
		{
			return (FileSystemNodeLabelsStore)mgr.store;
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Before()
		{
			mgr = new TestFileSystemNodeLabelsStore.MockNodeLabelManager();
			conf = new Configuration();
			conf.SetBoolean(YarnConfiguration.NodeLabelsEnabled, true);
			FilePath tempDir = FilePath.CreateTempFile("nlb", ".tmp");
			tempDir.Delete();
			tempDir.Mkdirs();
			tempDir.DeleteOnExit();
			conf.Set(YarnConfiguration.FsNodeLabelsStoreRootDir, tempDir.GetAbsolutePath());
			mgr.Init(conf);
			mgr.Start();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void After()
		{
			GetStore().fs.Delete(GetStore().fsWorkingPath, true);
			mgr.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRecoverWithMirror()
		{
			mgr.AddToCluserNodeLabels(ToSet("p1", "p2", "p3"));
			mgr.AddToCluserNodeLabels(ToSet("p4"));
			mgr.AddToCluserNodeLabels(ToSet("p5", "p6"));
			mgr.ReplaceLabelsOnNode((IDictionary)ImmutableMap.Of(ToNodeId("n1"), ToSet("p1"), 
				ToNodeId("n2"), ToSet("p2")));
			mgr.ReplaceLabelsOnNode((IDictionary)ImmutableMap.Of(ToNodeId("n3"), ToSet("p3"), 
				ToNodeId("n4"), ToSet("p4"), ToNodeId("n5"), ToSet("p5"), ToNodeId("n6"), ToSet(
				"p6"), ToNodeId("n7"), ToSet("p6")));
			/*
			* node -> partition p1: n1 p2: n2 p3: n3 p4: n4 p5: n5 p6: n6, n7
			*/
			mgr.RemoveFromClusterNodeLabels(ToSet("p1"));
			mgr.RemoveFromClusterNodeLabels(Arrays.AsList("p3", "p5"));
			/*
			* After removed p2: n2 p4: n4 p6: n6, n7
			*/
			// shutdown mgr and start a new mgr
			mgr.Stop();
			mgr = new TestFileSystemNodeLabelsStore.MockNodeLabelManager();
			mgr.Init(conf);
			mgr.Start();
			// check variables
			NUnit.Framework.Assert.AreEqual(3, mgr.GetClusterNodeLabels().Count);
			NUnit.Framework.Assert.IsTrue(mgr.GetClusterNodeLabels().ContainsAll(Arrays.AsList
				("p2", "p4", "p6")));
			AssertMapContains(mgr.GetNodeLabels(), ImmutableMap.Of(ToNodeId("n2"), ToSet("p2"
				), ToNodeId("n4"), ToSet("p4"), ToNodeId("n6"), ToSet("p6"), ToNodeId("n7"), ToSet
				("p6")));
			AssertLabelsToNodesEquals(mgr.GetLabelsToNodes(), ImmutableMap.Of("p6", ToSet(ToNodeId
				("n6"), ToNodeId("n7")), "p4", ToSet(ToNodeId("n4")), "p2", ToSet(ToNodeId("n2")
				)));
			// stutdown mgr and start a new mgr
			mgr.Stop();
			mgr = new TestFileSystemNodeLabelsStore.MockNodeLabelManager();
			mgr.Init(conf);
			mgr.Start();
			// check variables
			NUnit.Framework.Assert.AreEqual(3, mgr.GetClusterNodeLabels().Count);
			NUnit.Framework.Assert.IsTrue(mgr.GetClusterNodeLabels().ContainsAll(Arrays.AsList
				("p2", "p4", "p6")));
			AssertMapContains(mgr.GetNodeLabels(), ImmutableMap.Of(ToNodeId("n2"), ToSet("p2"
				), ToNodeId("n4"), ToSet("p4"), ToNodeId("n6"), ToSet("p6"), ToNodeId("n7"), ToSet
				("p6")));
			AssertLabelsToNodesEquals(mgr.GetLabelsToNodes(), ImmutableMap.Of("p6", ToSet(ToNodeId
				("n6"), ToNodeId("n7")), "p4", ToSet(ToNodeId("n4")), "p2", ToSet(ToNodeId("n2")
				)));
			mgr.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEditlogRecover()
		{
			mgr.AddToCluserNodeLabels(ToSet("p1", "p2", "p3"));
			mgr.AddToCluserNodeLabels(ToSet("p4"));
			mgr.AddToCluserNodeLabels(ToSet("p5", "p6"));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1"), ToNodeId("n2"
				), ToSet("p2")));
			mgr.ReplaceLabelsOnNode((IDictionary)ImmutableMap.Of(ToNodeId("n3"), ToSet("p3"), 
				ToNodeId("n4"), ToSet("p4"), ToNodeId("n5"), ToSet("p5"), ToNodeId("n6"), ToSet(
				"p6"), ToNodeId("n7"), ToSet("p6")));
			/*
			* node -> partition p1: n1 p2: n2 p3: n3 p4: n4 p5: n5 p6: n6, n7
			*/
			mgr.RemoveFromClusterNodeLabels(ToSet("p1"));
			mgr.RemoveFromClusterNodeLabels(Arrays.AsList("p3", "p5"));
			/*
			* After removed p2: n2 p4: n4 p6: n6, n7
			*/
			// shutdown mgr and start a new mgr
			mgr.Stop();
			mgr = new TestFileSystemNodeLabelsStore.MockNodeLabelManager();
			mgr.Init(conf);
			mgr.Start();
			// check variables
			NUnit.Framework.Assert.AreEqual(3, mgr.GetClusterNodeLabels().Count);
			NUnit.Framework.Assert.IsTrue(mgr.GetClusterNodeLabels().ContainsAll(Arrays.AsList
				("p2", "p4", "p6")));
			AssertMapContains(mgr.GetNodeLabels(), ImmutableMap.Of(ToNodeId("n2"), ToSet("p2"
				), ToNodeId("n4"), ToSet("p4"), ToNodeId("n6"), ToSet("p6"), ToNodeId("n7"), ToSet
				("p6")));
			AssertLabelsToNodesEquals(mgr.GetLabelsToNodes(), ImmutableMap.Of("p6", ToSet(ToNodeId
				("n6"), ToNodeId("n7")), "p4", ToSet(ToNodeId("n4")), "p2", ToSet(ToNodeId("n2")
				)));
			mgr.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSerilizationAfterRecovery()
		{
			//(timeout = 10000)
			mgr.AddToCluserNodeLabels(ToSet("p1", "p2", "p3"));
			mgr.AddToCluserNodeLabels(ToSet("p4"));
			mgr.AddToCluserNodeLabels(ToSet("p5", "p6"));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1"), ToNodeId("n2"
				), ToSet("p2")));
			mgr.ReplaceLabelsOnNode((IDictionary)ImmutableMap.Of(ToNodeId("n3"), ToSet("p3"), 
				ToNodeId("n4"), ToSet("p4"), ToNodeId("n5"), ToSet("p5"), ToNodeId("n6"), ToSet(
				"p6"), ToNodeId("n7"), ToSet("p6")));
			/*
			* node -> labels
			* p1: n1
			* p2: n2
			* p3: n3
			* p4: n4
			* p5: n5
			* p6: n6, n7
			*/
			mgr.RemoveFromClusterNodeLabels(ToSet("p1"));
			mgr.RemoveFromClusterNodeLabels(Arrays.AsList("p3", "p5"));
			/*
			* After removed
			* p2: n2
			* p4: n4
			* p6: n6, n7
			*/
			// shutdown mgr and start a new mgr
			mgr.Stop();
			mgr = new TestFileSystemNodeLabelsStore.MockNodeLabelManager();
			mgr.Init(conf);
			mgr.Start();
			// check variables
			NUnit.Framework.Assert.AreEqual(3, mgr.GetClusterNodeLabels().Count);
			NUnit.Framework.Assert.IsTrue(mgr.GetClusterNodeLabels().ContainsAll(Arrays.AsList
				("p2", "p4", "p6")));
			AssertMapContains(mgr.GetNodeLabels(), ImmutableMap.Of(ToNodeId("n2"), ToSet("p2"
				), ToNodeId("n4"), ToSet("p4"), ToNodeId("n6"), ToSet("p6"), ToNodeId("n7"), ToSet
				("p6")));
			AssertLabelsToNodesEquals(mgr.GetLabelsToNodes(), ImmutableMap.Of("p6", ToSet(ToNodeId
				("n6"), ToNodeId("n7")), "p4", ToSet(ToNodeId("n4")), "p2", ToSet(ToNodeId("n2")
				)));
			/*
			* Add label p7,p8 then shutdown
			*/
			mgr = new TestFileSystemNodeLabelsStore.MockNodeLabelManager();
			mgr.Init(conf);
			mgr.Start();
			mgr.AddToCluserNodeLabels(ToSet("p7", "p8"));
			mgr.Stop();
			/*
			* Restart, add label p9 and shutdown
			*/
			mgr = new TestFileSystemNodeLabelsStore.MockNodeLabelManager();
			mgr.Init(conf);
			mgr.Start();
			mgr.AddToCluserNodeLabels(ToSet("p9"));
			mgr.Stop();
			/*
			* Recovery, and see if p9 added
			*/
			mgr = new TestFileSystemNodeLabelsStore.MockNodeLabelManager();
			mgr.Init(conf);
			mgr.Start();
			// check variables
			NUnit.Framework.Assert.AreEqual(6, mgr.GetClusterNodeLabels().Count);
			NUnit.Framework.Assert.IsTrue(mgr.GetClusterNodeLabels().ContainsAll(Arrays.AsList
				("p2", "p4", "p6", "p7", "p8", "p9")));
			mgr.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRootMkdirOnInitStore()
		{
			FileSystem mockFs = Org.Mockito.Mockito.Mock<FileSystem>();
			FileSystemNodeLabelsStore mockStore = new _FileSystemNodeLabelsStore_283(mockFs, 
				mgr);
			mockStore.fs = mockFs;
			VerifyMkdirsCount(mockStore, true, 0);
			VerifyMkdirsCount(mockStore, false, 1);
			VerifyMkdirsCount(mockStore, true, 1);
			VerifyMkdirsCount(mockStore, false, 2);
		}

		private sealed class _FileSystemNodeLabelsStore_283 : FileSystemNodeLabelsStore
		{
			public _FileSystemNodeLabelsStore_283(FileSystem mockFs, CommonNodeLabelsManager 
				baseArg1)
				: base(baseArg1)
			{
				this.mockFs = mockFs;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void SetFileSystem(Configuration conf)
			{
				this.fs = mockFs;
			}

			private readonly FileSystem mockFs;
		}

		/// <exception cref="System.Exception"/>
		private void VerifyMkdirsCount(FileSystemNodeLabelsStore store, bool existsRetVal
			, int expectedNumOfCalls)
		{
			Org.Mockito.Mockito.When(store.fs.Exists(Org.Mockito.Mockito.Any<Path>())).ThenReturn
				(existsRetVal);
			store.Init(conf);
			Org.Mockito.Mockito.Verify(store.fs, Org.Mockito.Mockito.Times(expectedNumOfCalls
				)).Mkdirs(Org.Mockito.Mockito.Any<Path>());
		}
	}
}
