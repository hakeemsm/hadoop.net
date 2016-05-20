using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestFileContext
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestFileContext
			)));

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDefaultURIWithoutScheme()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set(org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY, "/");
			try
			{
				org.apache.hadoop.fs.FileContext.getFileContext(conf);
				NUnit.Framework.Assert.Fail(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.UnsupportedFileSystemException
					)) + " not thrown!");
			}
			catch (org.apache.hadoop.fs.UnsupportedFileSystemException ufse)
			{
				LOG.info("Expected exception: ", ufse);
			}
		}
	}
}
