using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Tests Job end notification in local mode.</summary>
	public class TestLocalMRNotification : NotificationTestCase
	{
		/// <exception cref="System.IO.IOException"/>
		public TestLocalMRNotification()
			: base(HadoopTestCase.LocalMr)
		{
		}
	}
}
