using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Net.Unix;
using Sharpen;

namespace Org.Apache.Hadoop.Tracing
{
	public class TestTraceAdmin
	{
		private static readonly string Newline = Runtime.GetProperty("line.separator");

		/// <exception cref="System.Exception"/>
		private string RunTraceCommand(TraceAdmin trace, params string[] cmd)
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			TextWriter ps = new TextWriter(baos);
			TextWriter oldStdout = System.Console.Out;
			TextWriter oldStderr = System.Console.Error;
			Runtime.SetOut(ps);
			Runtime.SetErr(ps);
			int ret = -1;
			try
			{
				ret = trace.Run(cmd);
			}
			finally
			{
				System.Console.Out.Flush();
				Runtime.SetOut(oldStdout);
				Runtime.SetErr(oldStderr);
			}
			return "ret:" + ret + ", " + baos.ToString();
		}

		private string GetHostPortForNN(MiniDFSCluster cluster)
		{
			return "127.0.0.1:" + cluster.GetNameNodePort();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateAndDestroySpanReceiver()
		{
			Configuration conf = new Configuration();
			conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsServerHtracePrefix + SpanReceiverHost.SpanReceiversConfSuffix
				, string.Empty);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			cluster.WaitActive();
			TemporarySocketDirectory tempDir = new TemporarySocketDirectory();
			string tracePath = new FilePath(tempDir.GetDir(), "tracefile").GetAbsolutePath();
			try
			{
				TraceAdmin trace = new TraceAdmin();
				trace.SetConf(conf);
				NUnit.Framework.Assert.AreEqual("ret:0, [no span receivers found]" + Newline, RunTraceCommand
					(trace, "-list", "-host", GetHostPortForNN(cluster)));
				NUnit.Framework.Assert.AreEqual("ret:0, Added trace span receiver 1 with " + "configuration local-file-span-receiver.path = "
					 + tracePath + Newline, RunTraceCommand(trace, "-add", "-host", GetHostPortForNN
					(cluster), "-class", "org.apache.htrace.impl.LocalFileSpanReceiver", "-Clocal-file-span-receiver.path="
					 + tracePath));
				string list = RunTraceCommand(trace, "-list", "-host", GetHostPortForNN(cluster));
				NUnit.Framework.Assert.IsTrue(list.StartsWith("ret:0"));
				NUnit.Framework.Assert.IsTrue(list.Contains("1   org.apache.htrace.impl.LocalFileSpanReceiver"
					));
				NUnit.Framework.Assert.AreEqual("ret:0, Removed trace span receiver 1" + Newline, 
					RunTraceCommand(trace, "-remove", "1", "-host", GetHostPortForNN(cluster)));
				NUnit.Framework.Assert.AreEqual("ret:0, [no span receivers found]" + Newline, RunTraceCommand
					(trace, "-list", "-host", GetHostPortForNN(cluster)));
				NUnit.Framework.Assert.AreEqual("ret:0, Added trace span receiver 2 with " + "configuration local-file-span-receiver.path = "
					 + tracePath + Newline, RunTraceCommand(trace, "-add", "-host", GetHostPortForNN
					(cluster), "-class", "LocalFileSpanReceiver", "-Clocal-file-span-receiver.path="
					 + tracePath));
				NUnit.Framework.Assert.AreEqual("ret:0, Removed trace span receiver 2" + Newline, 
					RunTraceCommand(trace, "-remove", "2", "-host", GetHostPortForNN(cluster)));
			}
			finally
			{
				cluster.Shutdown();
				tempDir.Close();
			}
		}
	}
}
