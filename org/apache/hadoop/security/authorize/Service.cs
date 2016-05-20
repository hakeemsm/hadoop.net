using Sharpen;

namespace org.apache.hadoop.security.authorize
{
	/// <summary>
	/// An abstract definition of <em>service</em> as related to
	/// Service Level Authorization for Hadoop.
	/// </summary>
	/// <remarks>
	/// An abstract definition of <em>service</em> as related to
	/// Service Level Authorization for Hadoop.
	/// Each service defines it's configuration key and also the necessary
	/// <see cref="java.security.Permission"/>
	/// required to access the service.
	/// </remarks>
	public class Service
	{
		private string key;

		private java.lang.Class protocol;

		public Service(string key, java.lang.Class protocol)
		{
			this.key = key;
			this.protocol = protocol;
		}

		/// <summary>Get the configuration key for the service.</summary>
		/// <returns>the configuration key for the service</returns>
		public virtual string getServiceKey()
		{
			return key;
		}

		/// <summary>Get the protocol for the service</summary>
		/// <returns>
		/// the
		/// <see cref="java.lang.Class{T}"/>
		/// for the protocol
		/// </returns>
		public virtual java.lang.Class getProtocol()
		{
			return protocol;
		}
	}
}
