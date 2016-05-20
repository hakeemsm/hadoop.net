using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestFileSystemInitialization
	{
		/// <summary>
		/// Check if FileSystem can be properly initialized if URLStreamHandlerFactory
		/// is registered.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testInitializationWithRegisteredStreamFactory()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			java.net.URL.setURLStreamHandlerFactory(new org.apache.hadoop.fs.FsUrlStreamHandlerFactory
				(conf));
			try
			{
				org.apache.hadoop.fs.FileSystem.getFileSystemClass("file", conf);
			}
			catch (System.IO.IOException)
			{
				// we might get an exception but this not related to infinite loop problem
				NUnit.Framework.Assert.IsFalse(false);
			}
		}
	}
}
