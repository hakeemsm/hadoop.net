using Javax.Management;
using Javax.Management.Openmbean;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;
using Sharpen.Management;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	public class TestSnapshotStatsMXBean
	{
		/// <summary>Test getting SnapshotStatsMXBean information</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSnapshotStatsMXBeanInfo()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = null;
			string pathName = "/snapshot";
			Path path = new Path(pathName);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Build();
				cluster.WaitActive();
				SnapshotManager sm = cluster.GetNamesystem().GetSnapshotManager();
				DistributedFileSystem dfs = (DistributedFileSystem)cluster.GetFileSystem();
				dfs.Mkdirs(path);
				dfs.AllowSnapshot(path);
				dfs.CreateSnapshot(path);
				MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
				ObjectName mxbeanName = new ObjectName("Hadoop:service=NameNode,name=SnapshotInfo"
					);
				CompositeData[] directories = (CompositeData[])mbs.GetAttribute(mxbeanName, "SnapshottableDirectories"
					);
				int numDirectories = Sharpen.Runtime.GetArrayLength(directories);
				NUnit.Framework.Assert.AreEqual(sm.GetNumSnapshottableDirs(), numDirectories);
				CompositeData[] snapshots = (CompositeData[])mbs.GetAttribute(mxbeanName, "Snapshots"
					);
				int numSnapshots = Sharpen.Runtime.GetArrayLength(snapshots);
				NUnit.Framework.Assert.AreEqual(sm.GetNumSnapshots(), numSnapshots);
				CompositeData d = (CompositeData)Sharpen.Runtime.GetArrayValue(directories, 0);
				CompositeData s = (CompositeData)Sharpen.Runtime.GetArrayValue(snapshots, 0);
				NUnit.Framework.Assert.IsTrue(((string)d.Get("path")).Contains(pathName));
				NUnit.Framework.Assert.IsTrue(((string)s.Get("snapshotDirectory")).Contains(pathName
					));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
