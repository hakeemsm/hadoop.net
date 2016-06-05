using System;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class TestOffsetUrlInputStream
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveOffset()
		{
			{
				//no offset
				string s = "http://test/Abc?Length=99";
				NUnit.Framework.Assert.AreEqual(s, WebHdfsFileSystem.RemoveOffsetParam(new Uri(s)
					).ToString());
			}
			{
				//no parameters
				string s = "http://test/Abc";
				NUnit.Framework.Assert.AreEqual(s, WebHdfsFileSystem.RemoveOffsetParam(new Uri(s)
					).ToString());
			}
			{
				//offset as first parameter
				string s = "http://test/Abc?offset=10&Length=99";
				NUnit.Framework.Assert.AreEqual("http://test/Abc?Length=99", WebHdfsFileSystem.RemoveOffsetParam
					(new Uri(s)).ToString());
			}
			{
				//offset as second parameter
				string s = "http://test/Abc?op=read&OFFset=10&Length=99";
				NUnit.Framework.Assert.AreEqual("http://test/Abc?op=read&Length=99", WebHdfsFileSystem
					.RemoveOffsetParam(new Uri(s)).ToString());
			}
			{
				//offset as last parameter
				string s = "http://test/Abc?Length=99&offset=10";
				NUnit.Framework.Assert.AreEqual("http://test/Abc?Length=99", WebHdfsFileSystem.RemoveOffsetParam
					(new Uri(s)).ToString());
			}
			{
				//offset as the only parameter
				string s = "http://test/Abc?offset=10";
				NUnit.Framework.Assert.AreEqual("http://test/Abc", WebHdfsFileSystem.RemoveOffsetParam
					(new Uri(s)).ToString());
			}
		}
	}
}
