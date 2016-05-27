using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	/// <summary>Singleton to get access to Http related configuration.</summary>
	public class HttpConfig
	{
		[System.Serializable]
		public sealed class Policy
		{
			public static readonly HttpConfig.Policy HttpOnly = new HttpConfig.Policy();

			public static readonly HttpConfig.Policy HttpsOnly = new HttpConfig.Policy();

			public static readonly HttpConfig.Policy HttpAndHttps = new HttpConfig.Policy();

			private static readonly HttpConfig.Policy[] Values = Values();

			public static HttpConfig.Policy FromString(string value)
			{
				foreach (HttpConfig.Policy p in HttpConfig.Policy.Values)
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(p.ToString(), value))
					{
						return p;
					}
				}
				return null;
			}

			public bool IsHttpEnabled()
			{
				return this == HttpConfig.Policy.HttpOnly || this == HttpConfig.Policy.HttpAndHttps;
			}

			public bool IsHttpsEnabled()
			{
				return this == HttpConfig.Policy.HttpsOnly || this == HttpConfig.Policy.HttpAndHttps;
			}
		}
	}
}
