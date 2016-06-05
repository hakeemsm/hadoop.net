using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestEditLogJournalFailures
	{
		private int editsPerformed = 0;

		private MiniDFSCluster cluster;

		private FileSystem fs;

		/// <summary>
		/// Create the mini cluster for testing and sub in a custom runtime so that
		/// edit log journal failures don't actually cause the JVM to exit.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetUpMiniCluster()
		{
			SetUpMiniCluster(new HdfsConfiguration(), true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetUpMiniCluster(Configuration conf, bool manageNameDfsDirs)
		{
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).ManageNameDfsDirs(manageNameDfsDirs
				).CheckExitOnShutdown(false).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void ShutDownMiniCluster()
		{
			if (fs != null)
			{
				fs.Close();
			}
			if (cluster != null)
			{
				try
				{
					cluster.Shutdown();
				}
				catch (ExitUtil.ExitException)
				{
				}
			}
		}

		// Ignore ExitExceptions as the tests may result in the
		// NameNode doing an immediate shutdown.
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleFailedEditsDirOnFlush()
		{
			NUnit.Framework.Assert.IsTrue(DoAnEdit());
			// Invalidate one edits journal.
			InvalidateEditsDirAtIndex(0, true, false);
			// The NN has not terminated (no ExitException thrown)
			NUnit.Framework.Assert.IsTrue(DoAnEdit());
			// A single journal failure should not result in a call to terminate
			NUnit.Framework.Assert.IsFalse(cluster.GetNameNode().IsInSafeMode());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAllEditsDirsFailOnFlush()
		{
			NUnit.Framework.Assert.IsTrue(DoAnEdit());
			// Invalidate both edits journals.
			InvalidateEditsDirAtIndex(0, true, false);
			InvalidateEditsDirAtIndex(1, true, false);
			// The NN has not terminated (no ExitException thrown)
			try
			{
				DoAnEdit();
				NUnit.Framework.Assert.Fail("The previous edit could not be synced to any persistent storage, "
					 + "should have halted the NN");
			}
			catch (RemoteException re)
			{
				NUnit.Framework.Assert.IsTrue(re.GetClassName().Contains("ExitException"));
				GenericTestUtils.AssertExceptionContains("Could not sync enough journals to persistent storage. "
					 + "Unsynced transactions: 1", re);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAllEditsDirFailOnWrite()
		{
			NUnit.Framework.Assert.IsTrue(DoAnEdit());
			// Invalidate both edits journals.
			InvalidateEditsDirAtIndex(0, true, true);
			InvalidateEditsDirAtIndex(1, true, true);
			// The NN has not terminated (no ExitException thrown)
			try
			{
				DoAnEdit();
				NUnit.Framework.Assert.Fail("The previous edit could not be synced to any persistent storage, "
					 + " should have halted the NN");
			}
			catch (RemoteException re)
			{
				NUnit.Framework.Assert.IsTrue(re.GetClassName().Contains("ExitException"));
				GenericTestUtils.AssertExceptionContains("Could not sync enough journals to persistent storage due to "
					 + "No journals available to flush. " + "Unsynced transactions: 1", re);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleFailedEditsDirOnSetReadyToFlush()
		{
			NUnit.Framework.Assert.IsTrue(DoAnEdit());
			// Invalidate one edits journal.
			InvalidateEditsDirAtIndex(0, false, false);
			// The NN has not terminated (no ExitException thrown)
			NUnit.Framework.Assert.IsTrue(DoAnEdit());
			// A single journal failure should not result in a call to terminate
			NUnit.Framework.Assert.IsFalse(cluster.GetNameNode().IsInSafeMode());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleRequiredFailedEditsDirOnSetReadyToFlush()
		{
			// Set one of the edits dirs to be required.
			string[] editsDirs = cluster.GetConfiguration(0).GetTrimmedStrings(DFSConfigKeys.
				DfsNamenodeNameDirKey);
			ShutDownMiniCluster();
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirRequiredKey, editsDirs[0]);
			conf.SetInt(DFSConfigKeys.DfsNamenodeEditsDirMinimumKey, 0);
			conf.SetInt(DFSConfigKeys.DfsNamenodeCheckedVolumesMinimumKey, 0);
			SetUpMiniCluster(conf, true);
			NUnit.Framework.Assert.IsTrue(DoAnEdit());
			// Invalidated the one required edits journal.
			InvalidateEditsDirAtIndex(0, false, false);
			JournalSet.JournalAndStream nonRequiredJas = GetJournalAndStream(1);
			EditLogFileOutputStream nonRequiredSpy = SpyOnStream(nonRequiredJas);
			// The NN has not terminated (no ExitException thrown)
			// ..and that the other stream is active.
			NUnit.Framework.Assert.IsTrue(nonRequiredJas.IsActive());
			try
			{
				DoAnEdit();
				NUnit.Framework.Assert.Fail("A single failure of a required journal should have halted the NN"
					);
			}
			catch (RemoteException re)
			{
				NUnit.Framework.Assert.IsTrue(re.GetClassName().Contains("ExitException"));
				GenericTestUtils.AssertExceptionContains("setReadyToFlush failed for required journal"
					, re);
			}
			// Since the required directory failed setReadyToFlush, and that
			// directory was listed prior to the non-required directory,
			// we should not call setReadyToFlush on the non-required
			// directory. Regression test for HDFS-2874.
			Org.Mockito.Mockito.Verify(nonRequiredSpy, Org.Mockito.Mockito.Never()).SetReadyToFlush
				();
			NUnit.Framework.Assert.IsFalse(nonRequiredJas.IsActive());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleRedundantFailedEditsDirOnSetReadyToFlush()
		{
			// Set up 4 name/edits dirs.
			ShutDownMiniCluster();
			Configuration conf = new HdfsConfiguration();
			string[] nameDirs = new string[4];
			for (int i = 0; i < nameDirs.Length; i++)
			{
				FilePath nameDir = new FilePath(PathUtils.GetTestDir(GetType()), "name-dir" + i);
				nameDir.Mkdirs();
				nameDirs[i] = nameDir.GetAbsolutePath();
			}
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, StringUtils.Join(nameDirs, ","));
			// Keep running unless there are less than 2 edits dirs remaining.
			conf.SetInt(DFSConfigKeys.DfsNamenodeEditsDirMinimumKey, 2);
			SetUpMiniCluster(conf, false);
			// All journals active.
			NUnit.Framework.Assert.IsTrue(DoAnEdit());
			// The NN has not terminated (no ExitException thrown)
			// Invalidate 1/4 of the redundant journals.
			InvalidateEditsDirAtIndex(0, false, false);
			NUnit.Framework.Assert.IsTrue(DoAnEdit());
			// The NN has not terminated (no ExitException thrown)
			// Invalidate 2/4 of the redundant journals.
			InvalidateEditsDirAtIndex(1, false, false);
			NUnit.Framework.Assert.IsTrue(DoAnEdit());
			// The NN has not terminated (no ExitException thrown)
			// Invalidate 3/4 of the redundant journals.
			InvalidateEditsDirAtIndex(2, false, false);
			try
			{
				DoAnEdit();
				NUnit.Framework.Assert.Fail("A failure of more than the minimum number of redundant journals "
					 + "should have halted ");
			}
			catch (RemoteException re)
			{
				NUnit.Framework.Assert.IsTrue(re.GetClassName().Contains("ExitException"));
				GenericTestUtils.AssertExceptionContains("Could not sync enough journals to persistent storage due to "
					 + "setReadyToFlush failed for too many journals. " + "Unsynced transactions: 1"
					, re);
			}
		}

		/// <summary>
		/// Replace the journal at index <code>index</code> with one that throws an
		/// exception on flush.
		/// </summary>
		/// <param name="index">the index of the journal to take offline.</param>
		/// <returns>the original <code>EditLogOutputStream</code> of the journal.</returns>
		/// <exception cref="System.IO.IOException"/>
		private void InvalidateEditsDirAtIndex(int index, bool failOnFlush, bool failOnWrite
			)
		{
			JournalSet.JournalAndStream jas = GetJournalAndStream(index);
			EditLogFileOutputStream spyElos = SpyOnStream(jas);
			if (failOnWrite)
			{
				Org.Mockito.Mockito.DoThrow(new IOException("fail on write()")).When(spyElos).Write
					((FSEditLogOp)Matchers.Any());
			}
			if (failOnFlush)
			{
				Org.Mockito.Mockito.DoThrow(new IOException("fail on flush()")).When(spyElos).Flush
					();
			}
			else
			{
				Org.Mockito.Mockito.DoThrow(new IOException("fail on setReadyToFlush()")).When(spyElos
					).SetReadyToFlush();
			}
		}

		private EditLogFileOutputStream SpyOnStream(JournalSet.JournalAndStream jas)
		{
			EditLogFileOutputStream elos = (EditLogFileOutputStream)jas.GetCurrentStream();
			EditLogFileOutputStream spyElos = Org.Mockito.Mockito.Spy(elos);
			jas.SetCurrentStreamForTests(spyElos);
			return spyElos;
		}

		/// <summary>Pull out one of the JournalAndStream objects from the edit log.</summary>
		private JournalSet.JournalAndStream GetJournalAndStream(int index)
		{
			FSImage fsimage = cluster.GetNamesystem().GetFSImage();
			FSEditLog editLog = fsimage.GetEditLog();
			return editLog.GetJournals()[index];
		}

		/// <summary>Do a mutative metadata operation on the file system.</summary>
		/// <returns>true if the operation was successful, false otherwise.</returns>
		/// <exception cref="System.IO.IOException"/>
		private bool DoAnEdit()
		{
			return fs.Mkdirs(new Path("/tmp", Sharpen.Extensions.ToString(editsPerformed++)));
		}
	}
}
