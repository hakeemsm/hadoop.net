using Org.Apache.Http.Client.Methods;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security.Client
{
	/// <summary>DelegationToken operations.</summary>
	[System.Serializable]
	public sealed class TimelineDelegationTokenOperation
	{
		public static readonly Org.Apache.Hadoop.Yarn.Security.Client.TimelineDelegationTokenOperation
			 Getdelegationtoken = new Org.Apache.Hadoop.Yarn.Security.Client.TimelineDelegationTokenOperation
			(HttpGet.MethodName, true);

		public static readonly Org.Apache.Hadoop.Yarn.Security.Client.TimelineDelegationTokenOperation
			 Renewdelegationtoken = new Org.Apache.Hadoop.Yarn.Security.Client.TimelineDelegationTokenOperation
			(HttpPut.MethodName, true);

		public static readonly Org.Apache.Hadoop.Yarn.Security.Client.TimelineDelegationTokenOperation
			 Canceldelegationtoken = new Org.Apache.Hadoop.Yarn.Security.Client.TimelineDelegationTokenOperation
			(HttpPut.MethodName, true);

		private string httpMethod;

		private bool requiresKerberosCredentials;

		private TimelineDelegationTokenOperation(string httpMethod, bool requiresKerberosCredentials
			)
		{
			// TODO: need think about which ops can be done without kerberos
			// credentials, for safety, we enforces all need kerberos credentials now.
			this.httpMethod = httpMethod;
			this.requiresKerberosCredentials = requiresKerberosCredentials;
		}

		public string GetHttpMethod()
		{
			return Org.Apache.Hadoop.Yarn.Security.Client.TimelineDelegationTokenOperation.httpMethod;
		}

		public bool RequiresKerberosCredentials()
		{
			return Org.Apache.Hadoop.Yarn.Security.Client.TimelineDelegationTokenOperation.requiresKerberosCredentials;
		}
	}
}
