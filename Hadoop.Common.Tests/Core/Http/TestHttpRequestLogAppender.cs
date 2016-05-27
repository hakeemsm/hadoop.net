using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	public class TestHttpRequestLogAppender
	{
		[NUnit.Framework.Test]
		public virtual void TestParameterPropagation()
		{
			HttpRequestLogAppender requestLogAppender = new HttpRequestLogAppender();
			requestLogAppender.SetFilename("jetty-namenode-yyyy_mm_dd.log");
			requestLogAppender.SetRetainDays(17);
			NUnit.Framework.Assert.AreEqual("Filename mismatch", "jetty-namenode-yyyy_mm_dd.log"
				, requestLogAppender.GetFilename());
			NUnit.Framework.Assert.AreEqual("Retain days mismatch", 17, requestLogAppender.GetRetainDays
				());
		}
	}
}
