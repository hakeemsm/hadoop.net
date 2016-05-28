using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Http POST operation parameter.</summary>
	public class PutOpParam : HttpOpParam<PutOpParam.OP>
	{
		/// <summary>Put operations.</summary>
		[System.Serializable]
		public sealed class OP : HttpOpParam.OP
		{
			public static readonly PutOpParam.OP Create = new PutOpParam.OP(true, HttpURLConnection
				.HttpCreated);

			public static readonly PutOpParam.OP Mkdirs = new PutOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly PutOpParam.OP Createsymlink = new PutOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly PutOpParam.OP Rename = new PutOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly PutOpParam.OP Setreplication = new PutOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly PutOpParam.OP Setowner = new PutOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly PutOpParam.OP Setpermission = new PutOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly PutOpParam.OP Settimes = new PutOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly PutOpParam.OP Renewdelegationtoken = new PutOpParam.OP(false
				, HttpURLConnection.HttpOk, true);

			public static readonly PutOpParam.OP Canceldelegationtoken = new PutOpParam.OP(false
				, HttpURLConnection.HttpOk, true);

			public static readonly PutOpParam.OP Modifyaclentries = new PutOpParam.OP(false, 
				HttpURLConnection.HttpOk);

			public static readonly PutOpParam.OP Removeaclentries = new PutOpParam.OP(false, 
				HttpURLConnection.HttpOk);

			public static readonly PutOpParam.OP Removedefaultacl = new PutOpParam.OP(false, 
				HttpURLConnection.HttpOk);

			public static readonly PutOpParam.OP Removeacl = new PutOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly PutOpParam.OP Setacl = new PutOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly PutOpParam.OP Setxattr = new PutOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly PutOpParam.OP Removexattr = new PutOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly PutOpParam.OP Createsnapshot = new PutOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly PutOpParam.OP Renamesnapshot = new PutOpParam.OP(false, HttpURLConnection
				.HttpOk);

			public static readonly PutOpParam.OP Null = new PutOpParam.OP(false, HttpURLConnection
				.HttpNotImplemented);

			internal readonly bool doOutputAndRedirect;

			internal readonly int expectedHttpResponseCode;

			internal readonly bool requireAuth;

			internal OP(bool doOutputAndRedirect, int expectedHttpResponseCode)
				: this(doOutputAndRedirect, expectedHttpResponseCode, false)
			{
			}

			internal OP(bool doOutputAndRedirect, int expectedHttpResponseCode, bool requireAuth
				)
			{
				this.doOutputAndRedirect = doOutputAndRedirect;
				this.expectedHttpResponseCode = expectedHttpResponseCode;
				this.requireAuth = requireAuth;
			}

			public HttpOpParam.Type GetType()
			{
				return HttpOpParam.Type.Put;
			}

			public bool GetRequireAuth()
			{
				return PutOpParam.OP.requireAuth;
			}

			public bool GetDoOutput()
			{
				return PutOpParam.OP.doOutputAndRedirect;
			}

			public bool GetRedirect()
			{
				return PutOpParam.OP.doOutputAndRedirect;
			}

			public int GetExpectedHttpResponseCode()
			{
				return PutOpParam.OP.expectedHttpResponseCode;
			}

			public string ToQueryString()
			{
				return Name + "=" + this;
			}
		}

		private static readonly EnumParam.Domain<PutOpParam.OP> Domain = new EnumParam.Domain
			<PutOpParam.OP>(Name, typeof(PutOpParam.OP));

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public PutOpParam(string str)
			: base(Domain, Domain.Parse(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
