using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestClose
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteAfterClose()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				byte[] data = Sharpen.Runtime.GetBytesForString("foo");
				FileSystem fs = FileSystem.Get(conf);
				OutputStream @out = fs.Create(new Path("/test"));
				@out.Write(data);
				@out.Close();
				try
				{
					// Should fail.
					@out.Write(data);
					NUnit.Framework.Assert.Fail("Should not have been able to write more data after file is closed."
						);
				}
				catch (ClosedChannelException)
				{
				}
				// We got the correct exception. Ignoring.
				// Should succeed. Double closes are OK.
				@out.Close();
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
