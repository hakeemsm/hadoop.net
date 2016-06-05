using Javax.Servlet;
using Javax.Servlet.Http;


namespace Org.Apache.Hadoop.Http
{
	public class NoCacheFilter : Filter
	{
		/// <exception cref="Javax.Servlet.ServletException"/>
		public virtual void Init(FilterConfig filterConfig)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Servlet.ServletException"/>
		public virtual void DoFilter(ServletRequest req, ServletResponse res, FilterChain
			 chain)
		{
			HttpServletResponse httpRes = (HttpServletResponse)res;
			httpRes.SetHeader("Cache-Control", "no-cache");
			long now = Runtime.CurrentTimeMillis();
			httpRes.AddDateHeader("Expires", now);
			httpRes.AddDateHeader("Date", now);
			httpRes.AddHeader("Pragma", "no-cache");
			chain.DoFilter(req, res);
		}

		public virtual void Destroy()
		{
		}
	}
}
