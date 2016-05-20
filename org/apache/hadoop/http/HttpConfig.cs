using Sharpen;

namespace org.apache.hadoop.http
{
	/// <summary>Singleton to get access to Http related configuration.</summary>
	public class HttpConfig
	{
		[System.Serializable]
		public sealed class Policy
		{
			public static readonly org.apache.hadoop.http.HttpConfig.Policy HTTP_ONLY = new org.apache.hadoop.http.HttpConfig.Policy
				();

			public static readonly org.apache.hadoop.http.HttpConfig.Policy HTTPS_ONLY = new 
				org.apache.hadoop.http.HttpConfig.Policy();

			public static readonly org.apache.hadoop.http.HttpConfig.Policy HTTP_AND_HTTPS = 
				new org.apache.hadoop.http.HttpConfig.Policy();

			private static readonly org.apache.hadoop.http.HttpConfig.Policy[] VALUES = values
				();

			public static org.apache.hadoop.http.HttpConfig.Policy fromString(string value)
			{
				foreach (org.apache.hadoop.http.HttpConfig.Policy p in org.apache.hadoop.http.HttpConfig.Policy
					.VALUES)
				{
					if (Sharpen.Runtime.equalsIgnoreCase(p.ToString(), value))
					{
						return p;
					}
				}
				return null;
			}

			public bool isHttpEnabled()
			{
				return this == org.apache.hadoop.http.HttpConfig.Policy.HTTP_ONLY || this == org.apache.hadoop.http.HttpConfig.Policy
					.HTTP_AND_HTTPS;
			}

			public bool isHttpsEnabled()
			{
				return this == org.apache.hadoop.http.HttpConfig.Policy.HTTPS_ONLY || this == org.apache.hadoop.http.HttpConfig.Policy
					.HTTP_AND_HTTPS;
			}
		}
	}
}
