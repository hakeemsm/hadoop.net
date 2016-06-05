using System;
using System.IO;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.FS
{
	public class TestFileSystemInitialization
	{
		/// <summary>
		/// Check if FileSystem can be properly initialized if URLStreamHandlerFactory
		/// is registered.
		/// </summary>
		[Fact]
		public virtual void TestInitializationWithRegisteredStreamFactory()
		{
			Configuration conf = new Configuration();
			Uri.SetURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(conf));
			try
			{
				FileSystem.GetFileSystemClass("file", conf);
			}
			catch (IOException)
			{
				// we might get an exception but this not related to infinite loop problem
				NUnit.Framework.Assert.IsFalse(false);
			}
		}
	}
}
