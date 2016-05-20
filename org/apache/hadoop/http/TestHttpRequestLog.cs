using Sharpen;

namespace org.apache.hadoop.http
{
	public class TestHttpRequestLog
	{
		[NUnit.Framework.Test]
		public virtual void testAppenderUndefined()
		{
			org.mortbay.jetty.RequestLog requestLog = org.apache.hadoop.http.HttpRequestLog.getRequestLog
				("test");
			NUnit.Framework.Assert.IsNull("RequestLog should be null", requestLog);
		}

		[NUnit.Framework.Test]
		public virtual void testAppenderDefined()
		{
			org.apache.hadoop.http.HttpRequestLogAppender requestLogAppender = new org.apache.hadoop.http.HttpRequestLogAppender
				();
			requestLogAppender.setName("testrequestlog");
			org.apache.log4j.Logger.getLogger("http.requests.test").addAppender(requestLogAppender
				);
			org.mortbay.jetty.RequestLog requestLog = org.apache.hadoop.http.HttpRequestLog.getRequestLog
				("test");
			org.apache.log4j.Logger.getLogger("http.requests.test").removeAppender(requestLogAppender
				);
			NUnit.Framework.Assert.IsNotNull("RequestLog should not be null", requestLog);
			NUnit.Framework.Assert.AreEqual("Class mismatch", Sharpen.Runtime.getClassForType
				(typeof(org.mortbay.jetty.NCSARequestLog)), Sharpen.Runtime.getClassForObject(requestLog
				));
		}
	}
}
