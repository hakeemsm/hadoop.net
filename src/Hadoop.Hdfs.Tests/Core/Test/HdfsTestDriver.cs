using System;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	/// <summary>Driver for HDFS tests.</summary>
	/// <remarks>Driver for HDFS tests. The tests should NOT depend on map-reduce APIs.</remarks>
	public class HdfsTestDriver
	{
		private readonly ProgramDriver pgd;

		public HdfsTestDriver()
			: this(new ProgramDriver())
		{
		}

		public HdfsTestDriver(ProgramDriver pgd)
		{
			this.pgd = pgd;
			try
			{
				pgd.AddClass("dfsthroughput", typeof(BenchmarkThroughput), "measure hdfs throughput"
					);
				pgd.AddClass("minidfscluster", typeof(MiniDFSClusterManager), "Run a single-process mini DFS cluster"
					);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
		}

		public virtual void Run(string[] argv)
		{
			int exitCode = -1;
			try
			{
				exitCode = pgd.Run(argv);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
			System.Environment.Exit(exitCode);
		}

		public static void Main(string[] argv)
		{
			new Org.Apache.Hadoop.Test.HdfsTestDriver().Run(argv);
		}
	}
}
