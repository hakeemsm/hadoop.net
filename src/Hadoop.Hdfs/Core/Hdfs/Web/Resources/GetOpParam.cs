using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Http GET operation parameter.</summary>
	public class GetOpParam : HttpOpParam<GetOpParam.OP>
	{
		/// <summary>Get operations.</summary>
		[System.Serializable]
		public sealed class OP : HttpOpParam.OP
		{
			public static readonly GetOpParam.OP Open = new GetOpParam.OP(true, HttpURLConnection
				.HttpOk);

			public static readonly GetOpParam.OP Getfilestatus = new GetOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly GetOpParam.OP Liststatus = new GetOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly GetOpParam.OP Getcontentsummary = new GetOpParam.OP(false, 
				HttpURLConnection.HttpOk);

			public static readonly GetOpParam.OP Getfilechecksum = new GetOpParam.OP(true, HttpURLConnection
				.HttpOk);

			public static readonly GetOpParam.OP Gethomedirectory = new GetOpParam.OP(false, 
				HttpURLConnection.HttpOk);

			public static readonly GetOpParam.OP Getdelegationtoken = new GetOpParam.OP(false
				, HttpURLConnection.HttpOk, true);

			/// <summary>GET_BLOCK_LOCATIONS is a private unstable op.</summary>
			public static readonly GetOpParam.OP GetBlockLocations = new GetOpParam.OP(false, 
				HttpURLConnection.HttpOk);

			public static readonly GetOpParam.OP Getaclstatus = new GetOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly GetOpParam.OP Getxattrs = new GetOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly GetOpParam.OP Listxattrs = new GetOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly GetOpParam.OP Null = new GetOpParam.OP(false, HttpURLConnection
				.HttpNotImplemented);

			public static readonly GetOpParam.OP Checkaccess = new GetOpParam.OP(false, HttpURLConnection
				.HttpOk);

			internal readonly bool redirect;

			internal readonly int expectedHttpResponseCode;

			internal readonly bool requireAuth;

			internal OP(bool redirect, int expectedHttpResponseCode)
				: this(redirect, expectedHttpResponseCode, false)
			{
			}

			internal OP(bool redirect, int expectedHttpResponseCode, bool requireAuth)
			{
				this.redirect = redirect;
				this.expectedHttpResponseCode = expectedHttpResponseCode;
				this.requireAuth = requireAuth;
			}

			public HttpOpParam.Type GetType()
			{
				return HttpOpParam.Type.Get;
			}

			public bool GetRequireAuth()
			{
				return GetOpParam.OP.requireAuth;
			}

			public bool GetDoOutput()
			{
				return false;
			}

			public bool GetRedirect()
			{
				return GetOpParam.OP.redirect;
			}

			public int GetExpectedHttpResponseCode()
			{
				return GetOpParam.OP.expectedHttpResponseCode;
			}

			public string ToQueryString()
			{
				return Name + "=" + this;
			}
		}

		private static readonly EnumParam.Domain<GetOpParam.OP> Domain = new EnumParam.Domain
			<GetOpParam.OP>(Name, typeof(GetOpParam.OP));

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public GetOpParam(string str)
			: base(Domain, Domain.Parse(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
