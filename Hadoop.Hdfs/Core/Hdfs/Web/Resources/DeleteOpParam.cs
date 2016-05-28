using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Http DELETE operation parameter.</summary>
	public class DeleteOpParam : HttpOpParam<DeleteOpParam.OP>
	{
		/// <summary>Delete operations.</summary>
		[System.Serializable]
		public sealed class OP : HttpOpParam.OP
		{
			public static readonly DeleteOpParam.OP Delete = new DeleteOpParam.OP(HttpURLConnection
				.HttpOk);

			public static readonly DeleteOpParam.OP Deletesnapshot = new DeleteOpParam.OP(HttpURLConnection
				.HttpOk);

			public static readonly DeleteOpParam.OP Null = new DeleteOpParam.OP(HttpURLConnection
				.HttpNotImplemented);

			internal readonly int expectedHttpResponseCode;

			internal OP(int expectedHttpResponseCode)
			{
				this.expectedHttpResponseCode = expectedHttpResponseCode;
			}

			public HttpOpParam.Type GetType()
			{
				return HttpOpParam.Type.Delete;
			}

			public bool GetRequireAuth()
			{
				return false;
			}

			public bool GetDoOutput()
			{
				return false;
			}

			public bool GetRedirect()
			{
				return false;
			}

			public int GetExpectedHttpResponseCode()
			{
				return DeleteOpParam.OP.expectedHttpResponseCode;
			}

			public string ToQueryString()
			{
				return Name + "=" + this;
			}
		}

		private static readonly EnumParam.Domain<DeleteOpParam.OP> Domain = new EnumParam.Domain
			<DeleteOpParam.OP>(Name, typeof(DeleteOpParam.OP));

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public DeleteOpParam(string str)
			: base(Domain, Domain.Parse(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
