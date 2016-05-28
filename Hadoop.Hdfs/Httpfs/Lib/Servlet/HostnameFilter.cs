using System.Net;
using Javax.Servlet;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Servlet
{
	/// <summary>Filter that resolves the requester hostname.</summary>
	public class HostnameFilter : Filter
	{
		internal static readonly ThreadLocal<string> HostnameTl = new ThreadLocal<string>
			();

		private static readonly Logger log = LoggerFactory.GetLogger(typeof(HostnameFilter
			));

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

		/// <summary>Resolves the requester hostname and delegates the request to the chain.</summary>
		/// <remarks>
		/// Resolves the requester hostname and delegates the request to the chain.
		/// <p>
		/// The requester hostname is available via the
		/// <see cref="Get()"/>
		/// method.
		/// </remarks>
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
				string hostname;
				try
				{
					string address = request.GetRemoteAddr();
					if (address != null)
					{
						hostname = Sharpen.Extensions.GetAddressByName(address).ToString();
					}
					else
					{
						log.Warn("Request remote address is NULL");
						hostname = "???";
					}
				}
				catch (UnknownHostException ex)
				{
					log.Warn("Request remote address could not be resolved, {0}", ex.ToString(), ex);
					hostname = "???";
				}
				HostnameTl.Set(hostname);
				chain.DoFilter(request, response);
			}
			finally
			{
				HostnameTl.Remove();
			}
		}

		/// <summary>Returns the requester hostname.</summary>
		/// <returns>the requester hostname.</returns>
		public static string Get()
		{
			return HostnameTl.Get();
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
