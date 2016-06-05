using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	public class TestRPCCallBenchmark
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestBenchmarkWithWritable()
		{
			int rc = ToolRunner.Run(new RPCCallBenchmark(), new string[] { "--clientThreads", 
				"30", "--serverThreads", "30", "--time", "5", "--serverReaderThreads", "4", "--messageSize"
				, "1024", "--engine", "writable" });
			Assert.Equal(0, rc);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBenchmarkWithProto()
		{
			int rc = ToolRunner.Run(new RPCCallBenchmark(), new string[] { "--clientThreads", 
				"30", "--serverThreads", "30", "--time", "5", "--serverReaderThreads", "4", "--messageSize"
				, "1024", "--engine", "protobuf" });
			Assert.Equal(0, rc);
		}
	}
}
