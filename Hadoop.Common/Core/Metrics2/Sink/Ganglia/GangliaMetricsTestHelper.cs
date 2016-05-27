using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Sink.Ganglia
{
	/// <summary>Helper class in the same package as ganglia sinks to be used by unit tests
	/// 	</summary>
	public class GangliaMetricsTestHelper
	{
		/// <summary>
		/// Helper method to access package private method to set DatagramSocket
		/// needed for Unit test
		/// </summary>
		/// <param name="gangliaSink"/>
		/// <param name="datagramSocket"/>
		public static void SetDatagramSocket(AbstractGangliaSink gangliaSink, DatagramSocket
			 datagramSocket)
		{
			gangliaSink.SetDatagramSocket(datagramSocket);
		}
	}
}
