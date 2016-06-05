using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	public class TestHttpRequestLogAppender
	{
		[Fact]
		public virtual void TestParameterPropagation()
		{
			HttpRequestLogAppender requestLogAppender = new HttpRequestLogAppender();
			requestLogAppender.SetFilename("jetty-namenode-yyyy_mm_dd.log");
			requestLogAppender.SetRetainDays(17);
			Assert.Equal("Filename mismatch", "jetty-namenode-yyyy_mm_dd.log"
				, requestLogAppender.GetFilename());
			Assert.Equal("Retain days mismatch", 17, requestLogAppender.GetRetainDays
				());
		}
	}
}
