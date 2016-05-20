using Sharpen;

namespace org.apache.hadoop.http
{
	public class TestHttpRequestLogAppender
	{
		[NUnit.Framework.Test]
		public virtual void testParameterPropagation()
		{
			org.apache.hadoop.http.HttpRequestLogAppender requestLogAppender = new org.apache.hadoop.http.HttpRequestLogAppender
				();
			requestLogAppender.setFilename("jetty-namenode-yyyy_mm_dd.log");
			requestLogAppender.setRetainDays(17);
			NUnit.Framework.Assert.AreEqual("Filename mismatch", "jetty-namenode-yyyy_mm_dd.log"
				, requestLogAppender.getFilename());
			NUnit.Framework.Assert.AreEqual("Retain days mismatch", 17, requestLogAppender.getRetainDays
				());
		}
	}
}
