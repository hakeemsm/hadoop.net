using Sharpen;

namespace org.apache.hadoop.util
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
		public const string ERROR_JSON = "RemoteException";

		public const string ERROR_EXCEPTION_JSON = "exception";

		public const string ERROR_CLASSNAME_JSON = "javaClassName";

		public const string ERROR_MESSAGE_JSON = "message";

		private const string APPLICATION_JSON_MIME = "application/json";

		private static readonly string ENTER = Sharpen.Runtime.getProperty("line.separator"
			);

		/// <summary>Creates a HTTP servlet response serializing the exception in it as JSON.
		/// 	</summary>
		/// <param name="response">the servlet response</param>
		/// <param name="status">the error code to set in the response</param>
		/// <param name="ex">the exception to serialize in the response</param>
		/// <exception cref="System.IO.IOException">
		/// thrown if there was an error while creating the
		/// response
		/// </exception>
		public static void createServletExceptionResponse(javax.servlet.http.HttpServletResponse
			 response, int status, System.Exception ex)
		{
			response.setStatus(status);
			response.setContentType(APPLICATION_JSON_MIME);
			System.Collections.Generic.IDictionary<string, object> json = new java.util.LinkedHashMap
				<string, object>();
			json[ERROR_MESSAGE_JSON] = getOneLineMessage(ex);
			json[ERROR_EXCEPTION_JSON] = Sharpen.Runtime.getClassForObject(ex).getSimpleName(
				);
			json[ERROR_CLASSNAME_JSON] = Sharpen.Runtime.getClassForObject(ex).getName();
			System.Collections.Generic.IDictionary<string, object> jsonResponse = new java.util.LinkedHashMap
				<string, object>();
			jsonResponse[ERROR_JSON] = json;
			org.codehaus.jackson.map.ObjectMapper jsonMapper = new org.codehaus.jackson.map.ObjectMapper
				();
			System.IO.TextWriter writer = response.getWriter();
			jsonMapper.writerWithDefaultPrettyPrinter().writeValue(writer, jsonResponse);
			writer.flush();
		}

		/// <summary>Creates a HTTP JAX-RPC response serializing the exception in it as JSON.
		/// 	</summary>
		/// <param name="status">the error code to set in the response</param>
		/// <param name="ex">the exception to serialize in the response</param>
		/// <returns>the JAX-RPC response with the set error and JSON encoded exception</returns>
		public static javax.ws.rs.core.Response createJerseyExceptionResponse(javax.ws.rs.core.Response.Status
			 status, System.Exception ex)
		{
			System.Collections.Generic.IDictionary<string, object> json = new java.util.LinkedHashMap
				<string, object>();
			json[ERROR_MESSAGE_JSON] = getOneLineMessage(ex);
			json[ERROR_EXCEPTION_JSON] = Sharpen.Runtime.getClassForObject(ex).getSimpleName(
				);
			json[ERROR_CLASSNAME_JSON] = Sharpen.Runtime.getClassForObject(ex).getName();
			System.Collections.Generic.IDictionary<string, object> response = new java.util.LinkedHashMap
				<string, object>();
			response[ERROR_JSON] = json;
			return javax.ws.rs.core.Response.status(status).type(javax.ws.rs.core.MediaType.APPLICATION_JSON
				).entity(response).build();
		}

		private static string getOneLineMessage(System.Exception exception)
		{
			string message = exception.Message;
			if (message != null)
			{
				int i = message.IndexOf(ENTER);
				if (i > -1)
				{
					message = Sharpen.Runtime.substring(message, 0, i);
				}
			}
			return message;
		}

		// trick, riding on generics to throw an undeclared exception
		private static void throwEx(System.Exception ex)
		{
			org.apache.hadoop.util.HttpExceptionUtils.throwException<System.Exception>(ex);
		}

		/// <exception cref="E"/>
		private static void throwException<E>(System.Exception ex)
			where E : System.Exception
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
		public static void validateResponse(java.net.HttpURLConnection conn, int expectedStatus
			)
		{
			if (conn.getResponseCode() != expectedStatus)
			{
				System.Exception toThrow;
				java.io.InputStream es = null;
				try
				{
					es = conn.getErrorStream();
					org.codehaus.jackson.map.ObjectMapper mapper = new org.codehaus.jackson.map.ObjectMapper
						();
					System.Collections.IDictionary json = mapper.readValue<System.Collections.IDictionary
						>(es);
					json = (System.Collections.IDictionary)json[ERROR_JSON];
					string exClass = (string)json[ERROR_CLASSNAME_JSON];
					string exMsg = (string)json[ERROR_MESSAGE_JSON];
					if (exClass != null)
					{
						try
						{
							java.lang.ClassLoader cl = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.HttpExceptionUtils
								)).getClassLoader();
							java.lang.Class klass = cl.loadClass(exClass);
							java.lang.reflect.Constructor constr = klass.getConstructor(Sharpen.Runtime.getClassForType
								(typeof(string)));
							toThrow = (System.Exception)constr.newInstance(exMsg);
						}
						catch (System.Exception)
						{
							toThrow = new System.IO.IOException(string.format("HTTP status [%d], exception [%s], message [%s] "
								, conn.getResponseCode(), exClass, exMsg));
						}
					}
					else
					{
						string msg = (exMsg != null) ? exMsg : conn.getResponseMessage();
						toThrow = new System.IO.IOException(string.format("HTTP status [%d], message [%s]"
							, conn.getResponseCode(), msg));
					}
				}
				catch (System.Exception)
				{
					toThrow = new System.IO.IOException(string.format("HTTP status [%d], message [%s]"
						, conn.getResponseCode(), conn.getResponseMessage()));
				}
				finally
				{
					if (es != null)
					{
						try
						{
							es.close();
						}
						catch (System.IO.IOException)
						{
						}
					}
				}
				//ignore
				throwEx(toThrow);
			}
		}
	}
}
