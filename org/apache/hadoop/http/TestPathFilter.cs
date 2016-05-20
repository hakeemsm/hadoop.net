using Sharpen;

namespace org.apache.hadoop.http
{
	public class TestPathFilter : org.apache.hadoop.http.HttpServerFunctionalTest
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.HttpServer2
			)));

		internal static readonly System.Collections.Generic.ICollection<string> RECORDS = 
			new java.util.TreeSet<string>();

		/// <summary>A very simple filter that records accessed uri's</summary>
		public class RecordingFilter : javax.servlet.Filter
		{
			private javax.servlet.FilterConfig filterConfig = null;

			public virtual void init(javax.servlet.FilterConfig filterConfig)
			{
				this.filterConfig = filterConfig;
			}

			public virtual void destroy()
			{
				this.filterConfig = null;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="javax.servlet.ServletException"/>
			public virtual void doFilter(javax.servlet.ServletRequest request, javax.servlet.ServletResponse
				 response, javax.servlet.FilterChain chain)
			{
				if (filterConfig == null)
				{
					return;
				}
				string uri = ((javax.servlet.http.HttpServletRequest)request).getRequestURI();
				LOG.info("filtering " + uri);
				RECORDS.add(uri);
				chain.doFilter(request, response);
			}

			/// <summary>Configuration for RecordingFilter</summary>
			public class Initializer : org.apache.hadoop.http.FilterInitializer
			{
				public Initializer()
				{
				}

				public override void initFilter(org.apache.hadoop.http.FilterContainer container, 
					org.apache.hadoop.conf.Configuration conf)
				{
					container.addFilter("recording", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestPathFilter.RecordingFilter
						)).getName(), null);
				}
			}
		}

		/// <summary>access a url, ignoring some IOException such as the page does not exist</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static void access(string urlstring)
		{
			LOG.warn("access " + urlstring);
			java.net.URL url = new java.net.URL(urlstring);
			java.net.URLConnection connection = url.openConnection();
			connection.connect();
			try
			{
				java.io.BufferedReader @in = new java.io.BufferedReader(new java.io.InputStreamReader
					(connection.getInputStream()));
				try
				{
					for (; @in.readLine() != null; )
					{
					}
				}
				finally
				{
					@in.close();
				}
			}
			catch (System.IO.IOException ioe)
			{
				LOG.warn("urlstring=" + urlstring, ioe);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testPathSpecFilters()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			//start a http server with CountingFilter
			conf.set(org.apache.hadoop.http.HttpServer2.FILTER_INITIALIZER_PROPERTY, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.http.TestPathFilter.RecordingFilter.Initializer)).getName
				());
			string[] pathSpecs = new string[] { "/path", "/path/*" };
			org.apache.hadoop.http.HttpServer2 http = createTestServer(conf, pathSpecs);
			http.start();
			string baseURL = "/path";
			string baseSlashURL = "/path/";
			string addedURL = "/path/nodes";
			string addedSlashURL = "/path/nodes/";
			string longURL = "/path/nodes/foo/job";
			string rootURL = "/";
			string allURL = "/*";
			string[] filteredUrls = new string[] { baseURL, baseSlashURL, addedURL, addedSlashURL
				, longURL };
			string[] notFilteredUrls = new string[] { rootURL, allURL };
			// access the urls and verify our paths specs got added to the 
			// filters
			string prefix = "http://" + org.apache.hadoop.net.NetUtils.getHostPortString(http
				.getConnectorAddress(0));
			try
			{
				for (int i = 0; i < filteredUrls.Length; i++)
				{
					access(prefix + filteredUrls[i]);
				}
				for (int i_1 = 0; i_1 < notFilteredUrls.Length; i_1++)
				{
					access(prefix + notFilteredUrls[i_1]);
				}
			}
			finally
			{
				http.stop();
			}
			LOG.info("RECORDS = " + RECORDS);
			//verify records
			for (int i_2 = 0; i_2 < filteredUrls.Length; i_2++)
			{
				NUnit.Framework.Assert.IsTrue(RECORDS.remove(filteredUrls[i_2]));
			}
			NUnit.Framework.Assert.IsTrue(RECORDS.isEmpty());
		}
	}
}
