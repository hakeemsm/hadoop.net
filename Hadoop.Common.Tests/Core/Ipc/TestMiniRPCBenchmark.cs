using Org.Apache.Hadoop.Conf;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>
	/// Test
	/// <see cref="MiniRPCBenchmark"/>
	/// </summary>
	public class TestMiniRPCBenchmark
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSimple()
		{
			Configuration conf = new Configuration();
			conf.Set("hadoop.security.authentication", "simple");
			MiniRPCBenchmark mb = new MiniRPCBenchmark(Level.Debug);
			mb.RunMiniBenchmark(conf, 10, null, null);
		}
	}
}
