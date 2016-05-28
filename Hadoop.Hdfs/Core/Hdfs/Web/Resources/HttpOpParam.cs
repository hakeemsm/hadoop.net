using System;
using System.Collections.Generic;
using Javax.WS.RS.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Http operation parameter.</summary>
	public abstract class HttpOpParam<E> : EnumParam<E>
		where E : Enum<E>
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "op";

		/// <summary>Default parameter value.</summary>
		public const string Default = Null;

		/// <summary>Http operation types</summary>
		public enum Type
		{
			Get,
			Put,
			Post,
			Delete
		}

		/// <summary>Http operation interface.</summary>
		public interface OP
		{
			/// <returns>the Http operation type.</returns>
			HttpOpParam.Type GetType();

			/// <returns>true if the operation cannot use a token</returns>
			bool GetRequireAuth();

			/// <returns>true if the operation will do output.</returns>
			bool GetDoOutput();

			/// <returns>true if the operation will be redirected.</returns>
			bool GetRedirect();

			/// <returns>true the expected http response code.</returns>
			int GetExpectedHttpResponseCode();

			/// <returns>a URI query string.</returns>
			string ToQueryString();
		}

		/// <summary>Expects HTTP response 307 "Temporary Redirect".</summary>
		public class TemporaryRedirectOp : HttpOpParam.OP
		{
			internal static readonly HttpOpParam.TemporaryRedirectOp Create = new HttpOpParam.TemporaryRedirectOp
				(PutOpParam.OP.Create);

			internal static readonly HttpOpParam.TemporaryRedirectOp Append = new HttpOpParam.TemporaryRedirectOp
				(PostOpParam.OP.Append);

			internal static readonly HttpOpParam.TemporaryRedirectOp Open = new HttpOpParam.TemporaryRedirectOp
				(GetOpParam.OP.Open);

			internal static readonly HttpOpParam.TemporaryRedirectOp Getfilechecksum = new HttpOpParam.TemporaryRedirectOp
				(GetOpParam.OP.Getfilechecksum);

			internal static readonly IList<HttpOpParam.TemporaryRedirectOp> values = Sharpen.Collections
				.UnmodifiableList(Arrays.AsList(Create, Append, Open, Getfilechecksum));

			/// <summary>Get an object for the given op.</summary>
			public static HttpOpParam.TemporaryRedirectOp ValueOf(HttpOpParam.OP op)
			{
				foreach (HttpOpParam.TemporaryRedirectOp t in values)
				{
					if (op == t.op)
					{
						return t;
					}
				}
				throw new ArgumentException(op + " not found.");
			}

			private readonly HttpOpParam.OP op;

			private TemporaryRedirectOp(HttpOpParam.OP op)
			{
				this.op = op;
			}

			public virtual HttpOpParam.Type GetType()
			{
				return op.GetType();
			}

			public virtual bool GetRequireAuth()
			{
				return op.GetRequireAuth();
			}

			public virtual bool GetDoOutput()
			{
				return false;
			}

			public virtual bool GetRedirect()
			{
				return false;
			}

			/// <summary>Override the original expected response with "Temporary Redirect".</summary>
			public virtual int GetExpectedHttpResponseCode()
			{
				return Response.Status.TemporaryRedirect.GetStatusCode();
			}

			public virtual string ToQueryString()
			{
				return op.ToQueryString();
			}
		}

		/// <returns>the parameter value as a string</returns>
		public override string GetValueString()
		{
			return value.ToString();
		}

		internal HttpOpParam(EnumParam.Domain<E> domain, E value)
			: base(domain, value)
		{
		}
	}
}
