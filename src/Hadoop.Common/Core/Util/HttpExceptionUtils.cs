using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Javax.Servlet.Http;
using Javax.WS.RS.Core;
using Org.Codehaus.Jackson.Map;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// HTTP utility class to help propagate server side exception to the client
	/// over HTTP as a JSON payload.
	/// </summary>
	/// <remarks>
	/// HTTP utility class to help propagate server side exception to the client
	/// over HTTP as a JSON payload.
	/// <p/>
	/// It creates HTTP Servlet and JAX-RPC error responses including details of the
	/// exception that allows a client to recreate the remote exception.
	/// <p/>
	/// It parses HTTP client connections and recreates the exception.
	/// </remarks>
	public class HttpExceptionUtils
	{
		public const string ErrorJson = "RemoteException";

		public const string ErrorExceptionJson = "exception";

		public const string ErrorClassnameJson = "javaClassName";

		public const string ErrorMessageJson = "message";

		private const string ApplicationJsonMime = "application/json";

		private static readonly string Enter = Runtime.GetProperty("line.separator");

		/// <summary>Creates a HTTP servlet response serializing the exception in it as JSON.
		/// 	</summary>
		/// <param name="response">the servlet response</param>
		/// <param name="status">the error code to set in the response</param>
		/// <param name="ex">the exception to serialize in the response</param>
		/// <exception cref="System.IO.IOException">
		/// thrown if there was an error while creating the
		/// response
		/// </exception>
		public static void CreateServletExceptionResponse(HttpServletResponse response, int
			 status, Exception ex)
		{
			response.SetStatus(status);
			response.SetContentType(ApplicationJsonMime);
			IDictionary<string, object> json = new LinkedHashMap<string, object>();
			json[ErrorMessageJson] = GetOneLineMessage(ex);
			json[ErrorExceptionJson] = ex.GetType().Name;
			json[ErrorClassnameJson] = ex.GetType().FullName;
			IDictionary<string, object> jsonResponse = new LinkedHashMap<string, object>();
			jsonResponse[ErrorJson] = json;
			ObjectMapper jsonMapper = new ObjectMapper();
			TextWriter writer = response.GetWriter();
			jsonMapper.WriterWithDefaultPrettyPrinter().WriteValue(writer, jsonResponse);
			writer.Flush();
		}

		/// <summary>Creates a HTTP JAX-RPC response serializing the exception in it as JSON.
		/// 	</summary>
		/// <param name="status">the error code to set in the response</param>
		/// <param name="ex">the exception to serialize in the response</param>
		/// <returns>the JAX-RPC response with the set error and JSON encoded exception</returns>
		public static Response CreateJerseyExceptionResponse(Response.Status status, Exception
			 ex)
		{
			IDictionary<string, object> json = new LinkedHashMap<string, object>();
			json[ErrorMessageJson] = GetOneLineMessage(ex);
			json[ErrorExceptionJson] = ex.GetType().Name;
			json[ErrorClassnameJson] = ex.GetType().FullName;
			IDictionary<string, object> response = new LinkedHashMap<string, object>();
			response[ErrorJson] = json;
			return Response.Status(status).Type(MediaType.ApplicationJson).Entity(response).Build
				();
		}

		private static string GetOneLineMessage(Exception exception)
		{
			string message = exception.Message;
			if (message != null)
			{
				int i = message.IndexOf(Enter);
				if (i > -1)
				{
					message = Runtime.Substring(message, 0, i);
				}
			}
			return message;
		}

		// trick, riding on generics to throw an undeclared exception
		private static void ThrowEx(Exception ex)
		{
			HttpExceptionUtils.ThrowException<RuntimeException>(ex);
		}

		/// <exception cref="E"/>
		private static void ThrowException<E>(Exception ex)
			where E : Exception
		{
			throw (E)ex;
		}

		/// <summary>
		/// Validates the status of an <code>HttpURLConnection</code> against an
		/// expected HTTP status code.
		/// </summary>
		/// <remarks>
		/// Validates the status of an <code>HttpURLConnection</code> against an
		/// expected HTTP status code. If the current status code is not the expected
		/// one it throws an exception with a detail message using Server side error
		/// messages if available.
		/// <p/>
		/// <b>NOTE:</b> this method will throw the deserialized exception even if not
		/// declared in the <code>throws</code> of the method signature.
		/// </remarks>
		/// <param name="conn">the <code>HttpURLConnection</code>.</param>
		/// <param name="expectedStatus">the expected HTTP status code.</param>
		/// <exception cref="System.IO.IOException">
		/// thrown if the current status code does not match the
		/// expected one.
		/// </exception>
		public static void ValidateResponse(HttpURLConnection conn, int expectedStatus)
		{
			if (conn.GetResponseCode() != expectedStatus)
			{
				Exception toThrow;
				InputStream es = null;
				try
				{
					es = conn.GetErrorStream();
					ObjectMapper mapper = new ObjectMapper();
					IDictionary json = mapper.ReadValue<IDictionary>(es);
					json = (IDictionary)json[ErrorJson];
					string exClass = (string)json[ErrorClassnameJson];
					string exMsg = (string)json[ErrorMessageJson];
					if (exClass != null)
					{
						try
						{
							ClassLoader cl = typeof(HttpExceptionUtils).GetClassLoader();
							Type klass = cl.LoadClass(exClass);
							ConstructorInfo constr = klass.GetConstructor(typeof(string));
							toThrow = (Exception)constr.NewInstance(exMsg);
						}
						catch (Exception)
						{
							toThrow = new IOException(string.Format("HTTP status [%d], exception [%s], message [%s] "
								, conn.GetResponseCode(), exClass, exMsg));
						}
					}
					else
					{
						string msg = (exMsg != null) ? exMsg : conn.GetResponseMessage();
						toThrow = new IOException(string.Format("HTTP status [%d], message [%s]", conn.GetResponseCode
							(), msg));
					}
				}
				catch (Exception)
				{
					toThrow = new IOException(string.Format("HTTP status [%d], message [%s]", conn.GetResponseCode
						(), conn.GetResponseMessage()));
				}
				finally
				{
					if (es != null)
					{
						try
						{
							es.Close();
						}
						catch (IOException)
						{
						}
					}
				}
				//ignore
				ThrowEx(toThrow);
			}
		}
	}
}
