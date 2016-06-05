using System.Collections.Generic;
using Javax.Servlet;
using Javax.Servlet.Http;
using NUnit.Framework;


namespace Org.Apache.Hadoop.Security.Http
{
	public class TestCrossOriginFilter
	{
		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestSameOrigin()
		{
			// Setup the configuration settings of the server
			IDictionary<string, string> conf = new Dictionary<string, string>();
			conf[CrossOriginFilter.AllowedOrigins] = string.Empty;
			FilterConfig filterConfig = new TestCrossOriginFilter.FilterConfigTest(conf);
			// Origin is not specified for same origin requests
			HttpServletRequest mockReq = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(mockReq.GetHeader(CrossOriginFilter.Origin)).ThenReturn(
				null);
			// Objects to verify interactions based on request
			HttpServletResponse mockRes = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			FilterChain mockChain = Org.Mockito.Mockito.Mock<FilterChain>();
			// Object under test
			CrossOriginFilter filter = new CrossOriginFilter();
			filter.Init(filterConfig);
			filter.DoFilter(mockReq, mockRes, mockChain);
			Org.Mockito.Mockito.VerifyZeroInteractions(mockRes);
			Org.Mockito.Mockito.Verify(mockChain).DoFilter(mockReq, mockRes);
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestAllowAllOrigins()
		{
			// Setup the configuration settings of the server
			IDictionary<string, string> conf = new Dictionary<string, string>();
			conf[CrossOriginFilter.AllowedOrigins] = "*";
			FilterConfig filterConfig = new TestCrossOriginFilter.FilterConfigTest(conf);
			// Object under test
			CrossOriginFilter filter = new CrossOriginFilter();
			filter.Init(filterConfig);
			Assert.True(filter.AreOriginsAllowed("example.com"));
		}

		[Fact]
		public virtual void TestEncodeHeaders()
		{
			string validOrigin = "http://localhost:12345";
			string encodedValidOrigin = CrossOriginFilter.EncodeHeader(validOrigin);
			Assert.Equal("Valid origin encoding should match exactly", validOrigin
				, encodedValidOrigin);
			string httpResponseSplitOrigin = validOrigin + " \nSecondHeader: value";
			string encodedResponseSplitOrigin = CrossOriginFilter.EncodeHeader(httpResponseSplitOrigin
				);
			Assert.Equal("Http response split origin should be protected against"
				, validOrigin, encodedResponseSplitOrigin);
			// Test Origin List
			string validOriginList = "http://foo.example.com:12345 http://bar.example.com:12345";
			string encodedValidOriginList = CrossOriginFilter.EncodeHeader(validOriginList);
			Assert.Equal("Valid origin list encoding should match exactly"
				, validOriginList, encodedValidOriginList);
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestPatternMatchingOrigins()
		{
			// Setup the configuration settings of the server
			IDictionary<string, string> conf = new Dictionary<string, string>();
			conf[CrossOriginFilter.AllowedOrigins] = "*.example.com";
			FilterConfig filterConfig = new TestCrossOriginFilter.FilterConfigTest(conf);
			// Object under test
			CrossOriginFilter filter = new CrossOriginFilter();
			filter.Init(filterConfig);
			// match multiple sub-domains
			NUnit.Framework.Assert.IsFalse(filter.AreOriginsAllowed("example.com"));
			NUnit.Framework.Assert.IsFalse(filter.AreOriginsAllowed("foo:example.com"));
			Assert.True(filter.AreOriginsAllowed("foo.example.com"));
			Assert.True(filter.AreOriginsAllowed("foo.bar.example.com"));
			// First origin is allowed
			Assert.True(filter.AreOriginsAllowed("foo.example.com foo.nomatch.com"
				));
			// Second origin is allowed
			Assert.True(filter.AreOriginsAllowed("foo.nomatch.com foo.example.com"
				));
			// No origin in list is allowed
			NUnit.Framework.Assert.IsFalse(filter.AreOriginsAllowed("foo.nomatch1.com foo.nomatch2.com"
				));
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDisallowedOrigin()
		{
			// Setup the configuration settings of the server
			IDictionary<string, string> conf = new Dictionary<string, string>();
			conf[CrossOriginFilter.AllowedOrigins] = "example.com";
			FilterConfig filterConfig = new TestCrossOriginFilter.FilterConfigTest(conf);
			// Origin is not specified for same origin requests
			HttpServletRequest mockReq = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(mockReq.GetHeader(CrossOriginFilter.Origin)).ThenReturn(
				"example.org");
			// Objects to verify interactions based on request
			HttpServletResponse mockRes = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			FilterChain mockChain = Org.Mockito.Mockito.Mock<FilterChain>();
			// Object under test
			CrossOriginFilter filter = new CrossOriginFilter();
			filter.Init(filterConfig);
			filter.DoFilter(mockReq, mockRes, mockChain);
			Org.Mockito.Mockito.VerifyZeroInteractions(mockRes);
			Org.Mockito.Mockito.Verify(mockChain).DoFilter(mockReq, mockRes);
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDisallowedMethod()
		{
			// Setup the configuration settings of the server
			IDictionary<string, string> conf = new Dictionary<string, string>();
			conf[CrossOriginFilter.AllowedOrigins] = "example.com";
			FilterConfig filterConfig = new TestCrossOriginFilter.FilterConfigTest(conf);
			// Origin is not specified for same origin requests
			HttpServletRequest mockReq = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(mockReq.GetHeader(CrossOriginFilter.Origin)).ThenReturn(
				"example.com");
			Org.Mockito.Mockito.When(mockReq.GetHeader(CrossOriginFilter.AccessControlRequestMethod
				)).ThenReturn("DISALLOWED_METHOD");
			// Objects to verify interactions based on request
			HttpServletResponse mockRes = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			FilterChain mockChain = Org.Mockito.Mockito.Mock<FilterChain>();
			// Object under test
			CrossOriginFilter filter = new CrossOriginFilter();
			filter.Init(filterConfig);
			filter.DoFilter(mockReq, mockRes, mockChain);
			Org.Mockito.Mockito.VerifyZeroInteractions(mockRes);
			Org.Mockito.Mockito.Verify(mockChain).DoFilter(mockReq, mockRes);
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDisallowedHeader()
		{
			// Setup the configuration settings of the server
			IDictionary<string, string> conf = new Dictionary<string, string>();
			conf[CrossOriginFilter.AllowedOrigins] = "example.com";
			FilterConfig filterConfig = new TestCrossOriginFilter.FilterConfigTest(conf);
			// Origin is not specified for same origin requests
			HttpServletRequest mockReq = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(mockReq.GetHeader(CrossOriginFilter.Origin)).ThenReturn(
				"example.com");
			Org.Mockito.Mockito.When(mockReq.GetHeader(CrossOriginFilter.AccessControlRequestMethod
				)).ThenReturn("GET");
			Org.Mockito.Mockito.When(mockReq.GetHeader(CrossOriginFilter.AccessControlRequestHeaders
				)).ThenReturn("Disallowed-Header");
			// Objects to verify interactions based on request
			HttpServletResponse mockRes = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			FilterChain mockChain = Org.Mockito.Mockito.Mock<FilterChain>();
			// Object under test
			CrossOriginFilter filter = new CrossOriginFilter();
			filter.Init(filterConfig);
			filter.DoFilter(mockReq, mockRes, mockChain);
			Org.Mockito.Mockito.VerifyZeroInteractions(mockRes);
			Org.Mockito.Mockito.Verify(mockChain).DoFilter(mockReq, mockRes);
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCrossOriginFilter()
		{
			// Setup the configuration settings of the server
			IDictionary<string, string> conf = new Dictionary<string, string>();
			conf[CrossOriginFilter.AllowedOrigins] = "example.com";
			FilterConfig filterConfig = new TestCrossOriginFilter.FilterConfigTest(conf);
			// Origin is not specified for same origin requests
			HttpServletRequest mockReq = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(mockReq.GetHeader(CrossOriginFilter.Origin)).ThenReturn(
				"example.com");
			Org.Mockito.Mockito.When(mockReq.GetHeader(CrossOriginFilter.AccessControlRequestMethod
				)).ThenReturn("GET");
			Org.Mockito.Mockito.When(mockReq.GetHeader(CrossOriginFilter.AccessControlRequestHeaders
				)).ThenReturn("X-Requested-With");
			// Objects to verify interactions based on request
			HttpServletResponse mockRes = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			FilterChain mockChain = Org.Mockito.Mockito.Mock<FilterChain>();
			// Object under test
			CrossOriginFilter filter = new CrossOriginFilter();
			filter.Init(filterConfig);
			filter.DoFilter(mockReq, mockRes, mockChain);
			Org.Mockito.Mockito.Verify(mockRes).SetHeader(CrossOriginFilter.AccessControlAllowOrigin
				, "example.com");
			Org.Mockito.Mockito.Verify(mockRes).SetHeader(CrossOriginFilter.AccessControlAllowCredentials
				, true.ToString());
			Org.Mockito.Mockito.Verify(mockRes).SetHeader(CrossOriginFilter.AccessControlAllowMethods
				, filter.GetAllowedMethodsHeader());
			Org.Mockito.Mockito.Verify(mockRes).SetHeader(CrossOriginFilter.AccessControlAllowHeaders
				, filter.GetAllowedHeadersHeader());
			Org.Mockito.Mockito.Verify(mockChain).DoFilter(mockReq, mockRes);
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		[Fact]
		public virtual void TestCrossOriginFilterAfterRestart()
		{
			// Setup the configuration settings of the server
			IDictionary<string, string> conf = new Dictionary<string, string>();
			conf[CrossOriginFilter.AllowedOrigins] = "example.com";
			conf[CrossOriginFilter.AllowedHeaders] = "X-Requested-With,Accept";
			conf[CrossOriginFilter.AllowedMethods] = "GET,POST";
			FilterConfig filterConfig = new TestCrossOriginFilter.FilterConfigTest(conf);
			// Object under test
			CrossOriginFilter filter = new CrossOriginFilter();
			filter.Init(filterConfig);
			//verify filter values
			Assert.True("Allowed headers do not match", string.CompareOrdinal
				(filter.GetAllowedHeadersHeader(), "X-Requested-With,Accept") == 0);
			Assert.True("Allowed methods do not match", string.CompareOrdinal
				(filter.GetAllowedMethodsHeader(), "GET,POST") == 0);
			Assert.True(filter.AreOriginsAllowed("example.com"));
			//destroy filter values and clear conf
			filter.Destroy();
			conf.Clear();
			// Setup the configuration settings of the server
			conf[CrossOriginFilter.AllowedOrigins] = "newexample.com";
			conf[CrossOriginFilter.AllowedHeaders] = "Content-Type,Origin";
			conf[CrossOriginFilter.AllowedMethods] = "GET,HEAD";
			filterConfig = new TestCrossOriginFilter.FilterConfigTest(conf);
			//initialize filter
			filter.Init(filterConfig);
			//verify filter values
			Assert.True("Allowed headers do not match", string.CompareOrdinal
				(filter.GetAllowedHeadersHeader(), "Content-Type,Origin") == 0);
			Assert.True("Allowed methods do not match", string.CompareOrdinal
				(filter.GetAllowedMethodsHeader(), "GET,HEAD") == 0);
			Assert.True(filter.AreOriginsAllowed("newexample.com"));
			//destroy filter values
			filter.Destroy();
		}

		private class FilterConfigTest : FilterConfig
		{
			internal readonly IDictionary<string, string> map;

			internal FilterConfigTest(IDictionary<string, string> map)
			{
				this.map = map;
			}

			public virtual string GetFilterName()
			{
				return "test-filter";
			}

			public virtual string GetInitParameter(string key)
			{
				return map[key];
			}

			public virtual Enumeration<string> GetInitParameterNames()
			{
				return Collections.Enumeration(map.Keys);
			}

			public virtual ServletContext GetServletContext()
			{
				return null;
			}
		}
	}
}
