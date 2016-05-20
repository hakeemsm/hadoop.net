using Sharpen;

namespace org.apache.hadoop.ipc
{
	public class TestRPCCallBenchmark
	{
		/// <exception cref="System.Exception"/>
		public virtual void testBenchmarkWithWritable()
		{
			int rc = org.apache.hadoop.util.ToolRunner.run(new org.apache.hadoop.ipc.RPCCallBenchmark
				(), new string[] { "--clientThreads", "30", "--serverThreads", "30", "--time", "5"
				, "--serverReaderThreads", "4", "--messageSize", "1024", "--engine", "writable" }
				);
			NUnit.Framework.Assert.AreEqual(0, rc);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testBenchmarkWithProto()
		{
			int rc = org.apache.hadoop.util.ToolRunner.run(new org.apache.hadoop.ipc.RPCCallBenchmark
				(), new string[] { "--clientThreads", "30", "--serverThreads", "30", "--time", "5"
				, "--serverReaderThreads", "4", "--messageSize", "1024", "--engine", "protobuf" }
				);
			NUnit.Framework.Assert.AreEqual(0, rc);
		}
	}
}
