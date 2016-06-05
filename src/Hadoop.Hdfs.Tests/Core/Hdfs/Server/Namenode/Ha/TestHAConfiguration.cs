using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>
	/// Test cases that the HA configuration is reasonably validated and
	/// interpreted in various places.
	/// </summary>
	/// <remarks>
	/// Test cases that the HA configuration is reasonably validated and
	/// interpreted in various places. These should be proper unit tests
	/// which don't start daemons.
	/// </remarks>
	public class TestHAConfiguration
	{
		private readonly FSNamesystem fsn = Org.Mockito.Mockito.Mock<FSNamesystem>();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckpointerValidityChecks()
		{
			try
			{
				Configuration conf = new Configuration();
				new StandbyCheckpointer(conf, fsn);
				NUnit.Framework.Assert.Fail("Bad config did not throw an error");
			}
			catch (ArgumentException iae)
			{
				GenericTestUtils.AssertExceptionContains("Invalid URI for NameNode address", iae);
			}
		}

		private Configuration GetHAConf(string nsId, string host1, string host2)
		{
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsNameservices, nsId);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsHaNamenodesKeyPrefix, nsId), "nn1,nn2"
				);
			conf.Set(DFSConfigKeys.DfsHaNamenodeIdKey, "nn1");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, nsId, "nn1"
				), host1 + ":12345");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, nsId, "nn2"
				), host2 + ":12345");
			return conf;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetOtherNNHttpAddress()
		{
			// Use non-local addresses to avoid host address matching
			Configuration conf = GetHAConf("ns1", "1.2.3.1", "1.2.3.2");
			conf.Set(DFSConfigKeys.DfsNameserviceId, "ns1");
			// This is done by the NN before the StandbyCheckpointer is created
			NameNode.InitializeGenericKeys(conf, "ns1", "nn1");
			// Since we didn't configure the HTTP address, and the default is
			// 0.0.0.0, it should substitute the address from the RPC configuration
			// above.
			StandbyCheckpointer checkpointer = new StandbyCheckpointer(conf, fsn);
			NUnit.Framework.Assert.AreEqual(new Uri("http", "1.2.3.2", DFSConfigKeys.DfsNamenodeHttpPortDefault
				, string.Empty), checkpointer.GetActiveNNAddress());
		}

		/// <summary>
		/// Tests that the namenode edits dirs and shared edits dirs are gotten with
		/// duplicates removed
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestHAUniqueEditDirs()
		{
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, "file://edits/dir, " + "file://edits/shared/dir"
				);
			// overlapping
			conf.Set(DFSConfigKeys.DfsNamenodeSharedEditsDirKey, "file://edits/shared/dir");
			// getNamespaceEditsDirs removes duplicates across edits and shared.edits
			ICollection<URI> editsDirs = FSNamesystem.GetNamespaceEditsDirs(conf);
			NUnit.Framework.Assert.AreEqual(2, editsDirs.Count);
		}

		/// <summary>Test that the 2NN does not start if given a config with HA NNs.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSecondaryNameNodeDoesNotStart()
		{
			// Note we're not explicitly setting the nameservice Id in the
			// config as it is not required to be set and we want to test
			// that we can determine if HA is enabled when the nameservice Id
			// is not explicitly defined.
			Configuration conf = GetHAConf("ns1", "1.2.3.1", "1.2.3.2");
			try
			{
				new SecondaryNameNode(conf);
				NUnit.Framework.Assert.Fail("Created a 2NN with an HA config");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("Cannot use SecondaryNameNode in an HA cluster"
					, ioe);
			}
		}
	}
}
