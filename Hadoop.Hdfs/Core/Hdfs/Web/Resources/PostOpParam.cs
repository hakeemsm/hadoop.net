using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Http POST operation parameter.</summary>
	public class PostOpParam : HttpOpParam<PostOpParam.OP>
	{
		/// <summary>Post operations.</summary>
		[System.Serializable]
		public sealed class OP : HttpOpParam.OP
		{
			public static readonly PostOpParam.OP Append = new PostOpParam.OP(true, HttpURLConnection
				.HttpOk);

			public static readonly PostOpParam.OP Concat = new PostOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly PostOpParam.OP Truncate = new PostOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly PostOpParam.OP Null = new PostOpParam.OP(false, HttpURLConnection
				.HttpNotImplemented);

			internal readonly bool doOutputAndRedirect;

			internal readonly int expectedHttpResponseCode;

			internal OP(bool doOutputAndRedirect, int expectedHttpResponseCode)
			{
				this.doOutputAndRedirect = doOutputAndRedirect;
				this.expectedHttpResponseCode = expectedHttpResponseCode;
			}

			public HttpOpParam.Type GetType()
			{
				return HttpOpParam.Type.Post;
			}

			public bool GetRequireAuth()
			{
				return false;
			}

			public bool GetDoOutput()
			{
				return PostOpParam.OP.doOutputAndRedirect;
			}

			public bool GetRedirect()
			{
				return PostOpParam.OP.doOutputAndRedirect;
			}

			public int GetExpectedHttpResponseCode()
			{
				return PostOpParam.OP.expectedHttpResponseCode;
			}

			/// <returns>a URI query string.</returns>
			public string ToQueryString()
			{
				return Name + "=" + this;
			}
		}

		private static readonly EnumParam.Domain<PostOpParam.OP> Domain = new EnumParam.Domain
			<PostOpParam.OP>(Name, typeof(PostOpParam.OP));

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public PostOpParam(string str)
			: base(Domain, Domain.Parse(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
