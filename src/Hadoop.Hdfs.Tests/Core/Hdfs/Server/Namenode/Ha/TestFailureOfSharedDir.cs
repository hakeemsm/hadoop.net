using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestFailureOfSharedDir
	{
		/// <summary>
		/// Test that the shared edits dir is automatically added to the list of edits
		/// dirs that are marked required.
		/// </summary>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestSharedDirIsAutomaticallyMarkedRequired()
		{
			URI foo = new URI("file:/foo");
			URI bar = new URI("file:/bar");
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, Joiner.On(",").Join(foo, bar));
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirRequiredKey, foo.ToString());
			NUnit.Framework.Assert.IsFalse(FSNamesystem.GetRequiredNamespaceEditsDirs(conf).Contains
				(bar));
			conf.Set(DFSConfigKeys.DfsNamenodeSharedEditsDirKey, bar.ToString());
			ICollection<URI> requiredEditsDirs = FSNamesystem.GetRequiredNamespaceEditsDirs(conf
				);
			NUnit.Framework.Assert.IsTrue(Joiner.On(",").Join(requiredEditsDirs) + " does not contain "
				 + bar, requiredEditsDirs.Contains(bar));
		}

		/// <summary>Multiple shared edits directories is an invalid configuration.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleSharedDirsFails()
		{
			Configuration conf = new Configuration();
			URI sharedA = new URI("file:///shared-A");
			URI sharedB = new URI("file:///shared-B");
			URI localA = new URI("file:///local-A");
			conf.Set(DFSConfigKeys.DfsNamenodeSharedEditsDirKey, Joiner.On(",").Join(sharedA, 
				sharedB));
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, localA.ToString());
			try
			{
				FSNamesystem.GetNamespaceEditsDirs(conf);
				NUnit.Framework.Assert.Fail("Allowed multiple shared edits directories");
			}
			catch (IOException ioe)
			{
				NUnit.Framework.Assert.AreEqual("Multiple shared edits directories are not yet supported"
					, ioe.Message);
			}
		}

		/// <summary>
		/// Make sure that the shared edits dirs are listed before non-shared dirs
		/// when the configuration is parsed.
		/// </summary>
		/// <remarks>
		/// Make sure that the shared edits dirs are listed before non-shared dirs
		/// when the configuration is parsed. This ensures that the shared journals
		/// are synced before the local ones.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSharedDirsComeFirstInEditsList()
		{
			Configuration conf = new Configuration();
			URI sharedA = new URI("file:///shared-A");
			URI localA = new URI("file:///local-A");
			URI localB = new URI("file:///local-B");
			URI localC = new URI("file:///local-C");
			conf.Set(DFSConfigKeys.DfsNamenodeSharedEditsDirKey, sharedA.ToString());
			// List them in reverse order, to make sure they show up in
			// the order listed, regardless of lexical sort order.
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, Joiner.On(",").Join(localC, localB
				, localA));
			IList<URI> dirs = FSNamesystem.GetNamespaceEditsDirs(conf);
			NUnit.Framework.Assert.AreEqual("Shared dirs should come first, then local dirs, in the order "
				 + "they were listed in the configuration.", Joiner.On(",").Join(sharedA, localC
				, localB, localA), Joiner.On(",").Join(dirs));
		}

		/// <summary>
		/// Test that marking the shared edits dir as being "required" causes the NN to
		/// fail if that dir can't be accessed.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureOfSharedDir()
		{
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsNamenodeResourceCheckIntervalKey, 2000);
			// The shared edits dir will automatically be marked required.
			MiniDFSCluster cluster = null;
			FilePath sharedEditsDir = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
					()).NumDataNodes(0).CheckExitOnShutdown(false).Build();
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/test1")));
				// Blow away the shared edits dir.
				URI sharedEditsUri = cluster.GetSharedEditsDir(0, 1);
				sharedEditsDir = new FilePath(sharedEditsUri);
				NUnit.Framework.Assert.AreEqual(0, FileUtil.Chmod(sharedEditsDir.GetAbsolutePath(
					), "-w", true));
				Sharpen.Thread.Sleep(conf.GetLong(DFSConfigKeys.DfsNamenodeResourceCheckIntervalKey
					, DFSConfigKeys.DfsNamenodeResourceCheckIntervalDefault) * 2);
				NameNode nn1 = cluster.GetNameNode(1);
				NUnit.Framework.Assert.IsTrue(nn1.IsStandbyState());
				NUnit.Framework.Assert.IsFalse("StandBy NameNode should not go to SafeMode on resource unavailability"
					, nn1.IsInSafeMode());
				NameNode nn0 = cluster.GetNameNode(0);
				try
				{
					// Make sure that subsequent operations on the NN fail.
					nn0.GetRpcServer().RollEditLog();
					NUnit.Framework.Assert.Fail("Succeeded in rolling edit log despite shared dir being deleted"
						);
				}
				catch (ExitUtil.ExitException ee)
				{
					GenericTestUtils.AssertExceptionContains("finalize log segment 1, 3 failed for required journal"
						, ee);
				}
				// Check that none of the edits dirs rolled, since the shared edits
				// dir didn't roll. Regression test for HDFS-2874.
				foreach (URI editsUri in cluster.GetNameEditsDirs(0))
				{
					if (editsUri.Equals(sharedEditsUri))
					{
						continue;
					}
					FilePath editsDir = new FilePath(editsUri.GetPath());
					FilePath curDir = new FilePath(editsDir, "current");
					GenericTestUtils.AssertGlobEquals(curDir, "edits_.*", NNStorage.GetInProgressEditsFileName
						(1));
				}
			}
			finally
			{
				if (sharedEditsDir != null)
				{
					// without this test cleanup will fail
					FileUtil.Chmod(sharedEditsDir.GetAbsolutePath(), "+w", true);
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
