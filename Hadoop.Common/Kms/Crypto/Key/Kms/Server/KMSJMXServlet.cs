using Javax.Servlet.Http;
using Org.Apache.Hadoop.Jmx;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	[System.Serializable]
	public class KMSJMXServlet : JMXJsonServlet
	{
		/// <exception cref="System.IO.IOException"/>
		protected override bool IsInstrumentationAccessAllowed(HttpServletRequest request
			, HttpServletResponse response)
		{
			return true;
		}
	}
}
