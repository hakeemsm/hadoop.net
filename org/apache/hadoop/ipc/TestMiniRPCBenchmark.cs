using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// Test
	/// <see cref="MiniRPCBenchmark"/>
	/// </summary>
	public class TestMiniRPCBenchmark
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testSimple()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("hadoop.security.authentication", "simple");
			org.apache.hadoop.ipc.MiniRPCBenchmark mb = new org.apache.hadoop.ipc.MiniRPCBenchmark
				(org.apache.log4j.Level.DEBUG);
			mb.runMiniBenchmark(conf, 10, null, null);
		}
	}
}
