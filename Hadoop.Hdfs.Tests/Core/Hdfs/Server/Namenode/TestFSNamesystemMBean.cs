using System;
using System.Collections.Generic;
using Javax.Management;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Mortbay.Util.Ajax;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Class for testing
	/// <see cref="NameNodeMXBean"/>
	/// implementation
	/// </summary>
	public class TestFSNamesystemMBean
	{
		/// <summary>
		/// MBeanClient tries to access FSNamesystem/FSNamesystemState/NameNodeInfo
		/// JMX properties.
		/// </summary>
		/// <remarks>
		/// MBeanClient tries to access FSNamesystem/FSNamesystemState/NameNodeInfo
		/// JMX properties. If it can access all the properties, the test is
		/// considered successful.
		/// </remarks>
		private class MBeanClient : Sharpen.Thread
		{
			private bool succeeded = false;

			public override void Run()
			{
				try
				{
					MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
					// Metrics that belong to "FSNamesystem", these are metrics that
					// come from hadoop metrics framework for the class FSNamesystem.
					ObjectName mxbeanNamefsn = new ObjectName("Hadoop:service=NameNode,name=FSNamesystem"
						);
					// Metrics that belong to "FSNamesystemState".
					// These are metrics that FSNamesystem registers directly with MBeanServer.
					ObjectName mxbeanNameFsns = new ObjectName("Hadoop:service=NameNode,name=FSNamesystemState"
						);
					// Metrics that belong to "NameNodeInfo".
					// These are metrics that FSNamesystem registers directly with MBeanServer.
					ObjectName mxbeanNameNni = new ObjectName("Hadoop:service=NameNode,name=NameNodeInfo"
						);
					ICollection<ObjectName> mbeans = new HashSet<ObjectName>();
					mbeans.AddItem(mxbeanNamefsn);
					mbeans.AddItem(mxbeanNameFsns);
					mbeans.AddItem(mxbeanNameNni);
					foreach (ObjectName mbean in mbeans)
					{
						MBeanInfo attributes = mbs.GetMBeanInfo(mbean);
						foreach (MBeanAttributeInfo attributeInfo in attributes.GetAttributes())
						{
							mbs.GetAttribute(mbean, attributeInfo.GetName());
						}
					}
					succeeded = true;
				}
				catch (Exception)
				{
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Test()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Build();
				cluster.WaitActive();
				FSNamesystem fsn = cluster.GetNameNode().namesystem;
				MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
				ObjectName mxbeanName = new ObjectName("Hadoop:service=NameNode,name=FSNamesystemState"
					);
				string snapshotStats = (string)(mbs.GetAttribute(mxbeanName, "SnapshotStats"));
				IDictionary<string, object> stat = (IDictionary<string, object>)JSON.Parse(snapshotStats
					);
				NUnit.Framework.Assert.IsTrue(stat.Contains("SnapshottableDirectories") && (long)
					stat["SnapshottableDirectories"] == fsn.GetNumSnapshottableDirs());
				NUnit.Framework.Assert.IsTrue(stat.Contains("Snapshots") && (long)stat["Snapshots"
					] == fsn.GetNumSnapshots());
				object pendingDeletionBlocks = mbs.GetAttribute(mxbeanName, "PendingDeletionBlocks"
					);
				NUnit.Framework.Assert.IsNotNull(pendingDeletionBlocks);
				NUnit.Framework.Assert.IsTrue(pendingDeletionBlocks is long);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		// The test makes sure JMX request can be processed even if namesystem's
		// writeLock is owned by another thread.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWithFSNamesystemWriteLock()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = null;
			FSNamesystem fsn = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Build();
				cluster.WaitActive();
				fsn = cluster.GetNameNode().namesystem;
				fsn.WriteLock();
				TestFSNamesystemMBean.MBeanClient client = new TestFSNamesystemMBean.MBeanClient(
					);
				client.Start();
				client.Join(20000);
				NUnit.Framework.Assert.IsTrue("JMX calls are blocked when FSNamesystem's writerlock"
					 + "is owned by another thread", client.succeeded);
				client.Interrupt();
			}
			finally
			{
				if (fsn != null && fsn.HasWriteLock())
				{
					fsn.WriteUnlock();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
