using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Servlet
{
	/// <summary>Filter that sets request contextual information for the slf4j MDC.</summary>
	/// <remarks>
	/// Filter that sets request contextual information for the slf4j MDC.
	/// <p>
	/// It sets the following values:
	/// <ul>
	/// <li>hostname: if the
	/// <see cref="HostnameFilter"/>
	/// is present and configured
	/// before this filter</li>
	/// <li>user: the <code>HttpServletRequest.getUserPrincipal().getName()</code></li>
	/// <li>method: the HTTP method fo the request (GET, POST, ...)</li>
	/// <li>path: the path of the request URL</li>
	/// </ul>
	/// </remarks>
	public class MDCFilter : Filter
	{
		/// <summary>Initializes the filter.</summary>
		/// <remarks>
		/// Initializes the filter.
		/// <p>
		/// This implementation is a NOP.
		/// </remarks>
		/// <param name="config">filter configuration.</param>
		/// <exception cref="Javax.Servlet.ServletException">thrown if the filter could not be initialized.
		/// 	</exception>
		public virtual void Init(FilterConfig config)
		{
		}

		/// <summary>Sets the slf4j <code>MDC</code> and delegates the request to the chain.</summary>
		/// <param name="request">servlet request.</param>
		/// <param name="response">servlet response.</param>
		/// <param name="chain">filter chain.</param>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurrs.</exception>
		/// <exception cref="Javax.Servlet.ServletException">thrown if a servet error occurrs.
		/// 	</exception>
		public virtual void DoFilter(ServletRequest request, ServletResponse response, FilterChain
			 chain)
		{
			try
			{
				MDC.Clear();
				string hostname = HostnameFilter.Get();
				if (hostname != null)
				{
					MDC.Put("hostname", HostnameFilter.Get());
				}
				Principal principal = ((HttpServletRequest)request).GetUserPrincipal();
				string user = (principal != null) ? principal.GetName() : null;
				if (user != null)
				{
					MDC.Put("user", user);
				}
				MDC.Put("method", ((HttpServletRequest)request).GetMethod());
				MDC.Put("path", ((HttpServletRequest)request).GetPathInfo());
				chain.DoFilter(request, response);
			}
			finally
			{
				MDC.Clear();
			}
		}

		/// <summary>Destroys the filter.</summary>
		/// <remarks>
		/// Destroys the filter.
		/// <p>
		/// This implementation is a NOP.
		/// </remarks>
		public virtual void Destroy()
		{
		}
	}
}
