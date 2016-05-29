using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Cli
{
	public class TestClusterCLI
	{
		internal ByteArrayOutputStream sysOutStream;

		private TextWriter sysOut;

		internal ByteArrayOutputStream sysErrStream;

		private TextWriter sysErr;

		[SetUp]
		public virtual void Setup()
		{
			sysOutStream = new ByteArrayOutputStream();
			sysOut = Org.Mockito.Mockito.Spy(new TextWriter(sysOutStream));
			sysErrStream = new ByteArrayOutputStream();
			sysErr = Org.Mockito.Mockito.Spy(new TextWriter(sysErrStream));
			Runtime.SetOut(sysOut);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetClusterNodeLabels()
		{
			YarnClient client = Org.Mockito.Mockito.Mock<YarnClient>();
			Org.Mockito.Mockito.When(client.GetClusterNodeLabels()).ThenReturn(ImmutableSet.Of
				("label1", "label2"));
			ClusterCLI cli = new ClusterCLI();
			cli.SetClient(client);
			cli.SetSysOutPrintStream(sysOut);
			cli.SetSysErrPrintStream(sysErr);
			int rc = cli.Run(new string[] { ClusterCLI.Cmd, "-" + ClusterCLI.ListLabelsCmd });
			NUnit.Framework.Assert.AreEqual(0, rc);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.Write("Node Labels: label1,label2");
			pw.Close();
			Org.Mockito.Mockito.Verify(sysOut).WriteLine(baos.ToString("UTF-8"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetClusterNodeLabelsWithLocalAccess()
		{
			YarnClient client = Org.Mockito.Mockito.Mock<YarnClient>();
			Org.Mockito.Mockito.When(client.GetClusterNodeLabels()).ThenReturn(ImmutableSet.Of
				("remote1", "remote2"));
			ClusterCLI cli = new ClusterCLI();
			cli.SetClient(client);
			cli.SetSysOutPrintStream(sysOut);
			cli.SetSysErrPrintStream(sysErr);
			ClusterCLI.localNodeLabelsManager = Org.Mockito.Mockito.Mock<CommonNodeLabelsManager
				>();
			Org.Mockito.Mockito.When(ClusterCLI.localNodeLabelsManager.GetClusterNodeLabels()
				).ThenReturn(ImmutableSet.Of("local1", "local2"));
			int rc = cli.Run(new string[] { ClusterCLI.Cmd, "-" + ClusterCLI.ListLabelsCmd, "-"
				 + ClusterCLI.DirectlyAccessNodeLabelStore });
			NUnit.Framework.Assert.AreEqual(0, rc);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			// it should return local* instead of remote*
			pw.Write("Node Labels: local1,local2");
			pw.Close();
			Org.Mockito.Mockito.Verify(sysOut).WriteLine(baos.ToString("UTF-8"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetEmptyClusterNodeLabels()
		{
			YarnClient client = Org.Mockito.Mockito.Mock<YarnClient>();
			Org.Mockito.Mockito.When(client.GetClusterNodeLabels()).ThenReturn(new HashSet<string
				>());
			ClusterCLI cli = new ClusterCLI();
			cli.SetClient(client);
			cli.SetSysOutPrintStream(sysOut);
			cli.SetSysErrPrintStream(sysErr);
			int rc = cli.Run(new string[] { ClusterCLI.Cmd, "-" + ClusterCLI.ListLabelsCmd });
			NUnit.Framework.Assert.AreEqual(0, rc);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.Write("Node Labels: ");
			pw.Close();
			Org.Mockito.Mockito.Verify(sysOut).WriteLine(baos.ToString("UTF-8"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHelp()
		{
			ClusterCLI cli = new ClusterCLI();
			cli.SetSysOutPrintStream(sysOut);
			cli.SetSysErrPrintStream(sysErr);
			int rc = cli.Run(new string[] { "cluster", "--help" });
			NUnit.Framework.Assert.AreEqual(0, rc);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("usage: yarn cluster");
			pw.WriteLine(" -dnl,--directly-access-node-label-store   Directly access node label"
				);
			pw.WriteLine("                                           store, with this option, all"
				);
			pw.WriteLine("                                           node label related operations"
				);
			pw.WriteLine("                                           will NOT connect RM. Instead,"
				);
			pw.WriteLine("                                           they will access/modify stored"
				);
			pw.WriteLine("                                           node labels directly. By"
				);
			pw.WriteLine("                                           default, it is false (access"
				);
			pw.WriteLine("                                           via RM). AND PLEASE NOTE: if"
				);
			pw.WriteLine("                                           you configured");
			pw.WriteLine("                                           yarn.node-labels.fs-store.root-"
				);
			pw.WriteLine("                                           dir to a local directory"
				);
			pw.WriteLine("                                           (instead of NFS or HDFS), this"
				);
			pw.WriteLine("                                           option will only work when the"
				);
			pw.WriteLine("                                           command run on the machine"
				);
			pw.WriteLine("                                           where RM is running. Also, this"
				);
			pw.WriteLine("                                           option is UNSTABLE, could be"
				);
			pw.WriteLine("                                           removed in future releases."
				);
			pw.WriteLine(" -h,--help                                 Displays help for all commands."
				);
			pw.WriteLine(" -lnl,--list-node-labels                   List cluster node-label"
				);
			pw.WriteLine("                                           collection");
			pw.Close();
			Org.Mockito.Mockito.Verify(sysOut).WriteLine(baos.ToString("UTF-8"));
		}
	}
}
