using Sharpen;

namespace org.apache.hadoop.http
{
	public class TestServletFilter : org.apache.hadoop.http.HttpServerFunctionalTest
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.HttpServer2
			)));

		internal static volatile string uri = null;

		/// <summary>A very simple filter which record the uri filtered.</summary>
		public class SimpleFilter : javax.servlet.Filter
		{
			private javax.servlet.FilterConfig filterConfig = null;

			/// <exception cref="javax.servlet.ServletException"/>
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
				uri = ((javax.servlet.http.HttpServletRequest)request).getRequestURI();
				LOG.info("filtering " + uri);
				chain.doFilter(request, response);
			}

			/// <summary>Configuration for the filter</summary>
			public class Initializer : org.apache.hadoop.http.FilterInitializer
			{
				public Initializer()
				{
				}

				public override void initFilter(org.apache.hadoop.http.FilterContainer container, 
					org.apache.hadoop.conf.Configuration conf)
				{
					container.addFilter("simple", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestServletFilter.SimpleFilter
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
		public virtual void testServletFilter()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			//start a http server with CountingFilter
			conf.set(org.apache.hadoop.http.HttpServer2.FILTER_INITIALIZER_PROPERTY, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.http.TestServletFilter.SimpleFilter.Initializer)).getName
				());
			org.apache.hadoop.http.HttpServer2 http = createTestServer(conf);
			http.start();
			string fsckURL = "/fsck";
			string stacksURL = "/stacks";
			string ajspURL = "/a.jsp";
			string logURL = "/logs/a.log";
			string hadooplogoURL = "/static/hadoop-logo.jpg";
			string[] urls = new string[] { fsckURL, stacksURL, ajspURL, logURL, hadooplogoURL
				 };
			java.util.Random ran = new java.util.Random();
			int[] sequence = new int[50];
			//generate a random sequence and update counts 
			for (int i = 0; i < sequence.Length; i++)
			{
				sequence[i] = ran.nextInt(urls.Length);
			}
			//access the urls as the sequence
			string prefix = "http://" + org.apache.hadoop.net.NetUtils.getHostPortString(http
				.getConnectorAddress(0));
			try
			{
				for (int i_1 = 0; i_1 < sequence.Length; i_1++)
				{
					access(prefix + urls[sequence[i_1]]);
					//make sure everything except fsck get filtered
					if (sequence[i_1] == 0)
					{
						NUnit.Framework.Assert.AreEqual(null, uri);
					}
					else
					{
						NUnit.Framework.Assert.AreEqual(urls[sequence[i_1]], uri);
						uri = null;
					}
				}
			}
			finally
			{
				http.stop();
			}
		}

		public class ErrorFilter : org.apache.hadoop.http.TestServletFilter.SimpleFilter
		{
			/// <exception cref="javax.servlet.ServletException"/>
			public override void init(javax.servlet.FilterConfig arg0)
			{
				throw new javax.servlet.ServletException("Throwing the exception from Filter init"
					);
			}

			/// <summary>Configuration for the filter</summary>
			public class Initializer : org.apache.hadoop.http.FilterInitializer
			{
				public Initializer()
				{
				}

				public override void initFilter(org.apache.hadoop.http.FilterContainer container, 
					org.apache.hadoop.conf.Configuration conf)
				{
					container.addFilter("simple", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestServletFilter.ErrorFilter
						)).getName(), null);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testServletFilterWhenInitThrowsException()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			// start a http server with CountingFilter
			conf.set(org.apache.hadoop.http.HttpServer2.FILTER_INITIALIZER_PROPERTY, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.http.TestServletFilter.ErrorFilter.Initializer)).getName
				());
			org.apache.hadoop.http.HttpServer2 http = createTestServer(conf);
			try
			{
				http.start();
				NUnit.Framework.Assert.Fail("expecting exception");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.contains("Problem in starting http server. Server handlers failed"
					));
			}
		}

		/// <summary>
		/// Similar to the above test case, except that it uses a different API to add
		/// the filter.
		/// </summary>
		/// <remarks>
		/// Similar to the above test case, except that it uses a different API to add
		/// the filter. Regression test for HADOOP-8786.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testContextSpecificServletFilterWhenInitThrowsException()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.http.HttpServer2 http = createTestServer(conf);
			org.apache.hadoop.http.HttpServer2.defineFilter(http.webAppContext, "ErrorFilter"
				, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestServletFilter.ErrorFilter
				)).getName(), null, null);
			try
			{
				http.start();
				NUnit.Framework.Assert.Fail("expecting exception");
			}
			catch (System.IO.IOException e)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Unable to initialize WebAppContext"
					, e);
			}
		}
	}
}
