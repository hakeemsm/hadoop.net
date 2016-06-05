using System;
using System.Text;
using Org.Apache.Hadoop.Lib.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Lang
{
	/// <summary>
	/// Generic exception that requires error codes and uses the a message
	/// template from the error code.
	/// </summary>
	[System.Serializable]
	public class XException : Exception
	{
		/// <summary>Interface to define error codes.</summary>
		public interface ERROR
		{
			/// <summary>Returns the template for the error.</summary>
			/// <returns>
			/// the template for the error, the template must be in JDK
			/// <code>MessageFormat</code> syntax (using {#} positional parameters).
			/// </returns>
			string GetTemplate();
		}

		private XException.ERROR error;

		/// <summary>Private constructor used by the public constructors.</summary>
		/// <param name="error">error code.</param>
		/// <param name="message">error message.</param>
		/// <param name="cause">exception cause if any.</param>
		private XException(XException.ERROR error, string message, Exception cause)
			: base(message, cause)
		{
			this.error = error;
		}

		/// <summary>Creates an XException using another XException as cause.</summary>
		/// <remarks>
		/// Creates an XException using another XException as cause.
		/// <p>
		/// The error code and error message are extracted from the cause.
		/// </remarks>
		/// <param name="cause">exception cause.</param>
		public XException(XException cause)
			: this(cause.GetError(), cause.Message, cause)
		{
		}

		/// <summary>Creates an XException using the specified error code.</summary>
		/// <remarks>
		/// Creates an XException using the specified error code. The exception
		/// message is resolved using the error code template and the passed
		/// parameters.
		/// </remarks>
		/// <param name="error">error code for the XException.</param>
		/// <param name="params">
		/// parameters to use when creating the error message
		/// with the error code template.
		/// </param>
		public XException(XException.ERROR error, params object[] @params)
			: this(Check.NotNull(error, "error"), Format(error, @params), GetCause(@params))
		{
		}

		/// <summary>Returns the error code of the exception.</summary>
		/// <returns>the error code of the exception.</returns>
		public virtual XException.ERROR GetError()
		{
			return error;
		}

		/// <summary>Creates a message using a error message template and arguments.</summary>
		/// <remarks>
		/// Creates a message using a error message template and arguments.
		/// <p>
		/// The template must be in JDK <code>MessageFormat</code> syntax
		/// (using {#} positional parameters).
		/// </remarks>
		/// <param name="error">error code, to get the template from.</param>
		/// <param name="args">arguments to use for creating the message.</param>
		/// <returns>the resolved error message.</returns>
		private static string Format(XException.ERROR error, params object[] args)
		{
			string template = error.GetTemplate();
			if (template == null)
			{
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < args.Length; i++)
				{
					sb.Append(" {").Append(i).Append("}");
				}
				template = Sharpen.Runtime.DeleteCharAt(sb, 0).ToString();
			}
			return error + ": " + MessageFormat.Format(template, args);
		}

		/// <summary>
		/// Returns the last parameter if it is an instance of <code>Throwable</code>
		/// returns it else it returns NULL.
		/// </summary>
		/// <param name="params">parameters to look for a cause.</param>
		/// <returns>
		/// the last parameter if it is an instance of <code>Throwable</code>
		/// returns it else it returns NULL.
		/// </returns>
		private static Exception GetCause(params object[] @params)
		{
			Exception throwable = null;
			if (@params != null && @params.Length > 0 && @params[@params.Length - 1] is Exception)
			{
				throwable = (Exception)@params[@params.Length - 1];
			}
			return throwable;
		}
	}
}
