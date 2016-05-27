using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Javax.Servlet.Http;
using Javax.WS.RS.Core;
using NUnit.Framework;
using Org.Codehaus.Jackson.Map;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestHttpExceptionUtils
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateServletException()
		{
			StringWriter writer = new StringWriter();
			PrintWriter printWriter = new PrintWriter(writer);
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Mockito.Mockito.When(response.GetWriter()).ThenReturn(printWriter);
			int status = HttpServletResponse.ScInternalServerError;
			Exception ex = new IOException("Hello IOEX");
			HttpExceptionUtils.CreateServletExceptionResponse(response, status, ex);
			Org.Mockito.Mockito.Verify(response).SetStatus(status);
			Org.Mockito.Mockito.Verify(response).SetContentType(Org.Mockito.Mockito.Eq("application/json"
				));
			ObjectMapper mapper = new ObjectMapper();
			IDictionary json = mapper.ReadValue<IDictionary>(writer.ToString());
			json = (IDictionary)json[HttpExceptionUtils.ErrorJson];
			NUnit.Framework.Assert.AreEqual(typeof(IOException).FullName, json[HttpExceptionUtils
				.ErrorClassnameJson]);
			NUnit.Framework.Assert.AreEqual(typeof(IOException).Name, json[HttpExceptionUtils
				.ErrorExceptionJson]);
			NUnit.Framework.Assert.AreEqual("Hello IOEX", json[HttpExceptionUtils.ErrorMessageJson
				]);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateJerseyException()
		{
			Exception ex = new IOException("Hello IOEX");
			Response response = HttpExceptionUtils.CreateJerseyExceptionResponse(Response.Status
				.InternalServerError, ex);
			NUnit.Framework.Assert.AreEqual(Response.Status.InternalServerError.GetStatusCode
				(), response.GetStatus());
			Assert.AssertArrayEquals(Sharpen.Collections.ToArray(Arrays.AsList(MediaType.ApplicationJsonType
				)), Sharpen.Collections.ToArray(response.GetMetadata()["Content-Type"]));
			IDictionary entity = (IDictionary)response.GetEntity();
			entity = (IDictionary)entity[HttpExceptionUtils.ErrorJson];
			NUnit.Framework.Assert.AreEqual(typeof(IOException).FullName, entity[HttpExceptionUtils
				.ErrorClassnameJson]);
			NUnit.Framework.Assert.AreEqual(typeof(IOException).Name, entity[HttpExceptionUtils
				.ErrorExceptionJson]);
			NUnit.Framework.Assert.AreEqual("Hello IOEX", entity[HttpExceptionUtils.ErrorMessageJson
				]);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestValidateResponseOK()
		{
			HttpURLConnection conn = Org.Mockito.Mockito.Mock<HttpURLConnection>();
			Org.Mockito.Mockito.When(conn.GetResponseCode()).ThenReturn(HttpURLConnection.HttpCreated
				);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpCreated);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestValidateResponseFailNoErrorMessage()
		{
			HttpURLConnection conn = Org.Mockito.Mockito.Mock<HttpURLConnection>();
			Org.Mockito.Mockito.When(conn.GetResponseCode()).ThenReturn(HttpURLConnection.HttpBadRequest
				);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpCreated);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestValidateResponseNonJsonErrorMessage()
		{
			string msg = "stream";
			InputStream @is = new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString(msg)
				);
			HttpURLConnection conn = Org.Mockito.Mockito.Mock<HttpURLConnection>();
			Org.Mockito.Mockito.When(conn.GetErrorStream()).ThenReturn(@is);
			Org.Mockito.Mockito.When(conn.GetResponseMessage()).ThenReturn("msg");
			Org.Mockito.Mockito.When(conn.GetResponseCode()).ThenReturn(HttpURLConnection.HttpBadRequest
				);
			try
			{
				HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpCreated);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException ex)
			{
				NUnit.Framework.Assert.IsTrue(ex.Message.Contains("msg"));
				NUnit.Framework.Assert.IsTrue(ex.Message.Contains(string.Empty + HttpURLConnection
					.HttpBadRequest));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestValidateResponseJsonErrorKnownException()
		{
			IDictionary<string, object> json = new Dictionary<string, object>();
			json[HttpExceptionUtils.ErrorExceptionJson] = typeof(InvalidOperationException).Name;
			json[HttpExceptionUtils.ErrorClassnameJson] = typeof(InvalidOperationException).FullName;
			json[HttpExceptionUtils.ErrorMessageJson] = "EX";
			IDictionary<string, object> response = new Dictionary<string, object>();
			response[HttpExceptionUtils.ErrorJson] = json;
			ObjectMapper jsonMapper = new ObjectMapper();
			string msg = jsonMapper.WriteValueAsString(response);
			InputStream @is = new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString(msg)
				);
			HttpURLConnection conn = Org.Mockito.Mockito.Mock<HttpURLConnection>();
			Org.Mockito.Mockito.When(conn.GetErrorStream()).ThenReturn(@is);
			Org.Mockito.Mockito.When(conn.GetResponseMessage()).ThenReturn("msg");
			Org.Mockito.Mockito.When(conn.GetResponseCode()).ThenReturn(HttpURLConnection.HttpBadRequest
				);
			try
			{
				HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpCreated);
				NUnit.Framework.Assert.Fail();
			}
			catch (InvalidOperationException ex)
			{
				NUnit.Framework.Assert.AreEqual("EX", ex.Message);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestValidateResponseJsonErrorUnknownException()
		{
			IDictionary<string, object> json = new Dictionary<string, object>();
			json[HttpExceptionUtils.ErrorExceptionJson] = "FooException";
			json[HttpExceptionUtils.ErrorClassnameJson] = "foo.FooException";
			json[HttpExceptionUtils.ErrorMessageJson] = "EX";
			IDictionary<string, object> response = new Dictionary<string, object>();
			response[HttpExceptionUtils.ErrorJson] = json;
			ObjectMapper jsonMapper = new ObjectMapper();
			string msg = jsonMapper.WriteValueAsString(response);
			InputStream @is = new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString(msg)
				);
			HttpURLConnection conn = Org.Mockito.Mockito.Mock<HttpURLConnection>();
			Org.Mockito.Mockito.When(conn.GetErrorStream()).ThenReturn(@is);
			Org.Mockito.Mockito.When(conn.GetResponseMessage()).ThenReturn("msg");
			Org.Mockito.Mockito.When(conn.GetResponseCode()).ThenReturn(HttpURLConnection.HttpBadRequest
				);
			try
			{
				HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpCreated);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException ex)
			{
				NUnit.Framework.Assert.IsTrue(ex.Message.Contains("EX"));
				NUnit.Framework.Assert.IsTrue(ex.Message.Contains("foo.FooException"));
			}
		}
	}
}
