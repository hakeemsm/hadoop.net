using Sharpen;

namespace org.apache.hadoop.http
{
	public class NoCacheFilter : javax.servlet.Filter
	{
		/// <exception cref="javax.servlet.ServletException"/>
		public virtual void init(javax.servlet.FilterConfig filterConfig)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="javax.servlet.ServletException"/>
		public virtual void doFilter(javax.servlet.ServletRequest req, javax.servlet.ServletResponse
			 res, javax.servlet.FilterChain chain)
		{
			javax.servlet.http.HttpServletResponse httpRes = (javax.servlet.http.HttpServletResponse
				)res;
			httpRes.setHeader("Cache-Control", "no-cache");
			long now = Sharpen.Runtime.currentTimeMillis();
			httpRes.addDateHeader("Expires", now);
			httpRes.addDateHeader("Date", now);
			httpRes.addHeader("Pragma", "no-cache");
			chain.doFilter(req, res);
		}

		public virtual void destroy()
		{
		}
	}
}
