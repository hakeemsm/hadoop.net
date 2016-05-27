using Org.Apache.Log4j;
using Org.Mortbay.Jetty;
using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	public class TestHttpRequestLog
	{
		[NUnit.Framework.Test]
		public virtual void TestAppenderUndefined()
		{
			RequestLog requestLog = HttpRequestLog.GetRequestLog("test");
			NUnit.Framework.Assert.IsNull("RequestLog should be null", requestLog);
		}

		[NUnit.Framework.Test]
		public virtual void TestAppenderDefined()
		{
			HttpRequestLogAppender requestLogAppender = new HttpRequestLogAppender();
			requestLogAppender.SetName("testrequestlog");
			Logger.GetLogger("http.requests.test").AddAppender(requestLogAppender);
			RequestLog requestLog = HttpRequestLog.GetRequestLog("test");
			Logger.GetLogger("http.requests.test").RemoveAppender(requestLogAppender);
			NUnit.Framework.Assert.IsNotNull("RequestLog should not be null", requestLog);
			NUnit.Framework.Assert.AreEqual("Class mismatch", typeof(NCSARequestLog), requestLog
				.GetType());
		}
	}
}
