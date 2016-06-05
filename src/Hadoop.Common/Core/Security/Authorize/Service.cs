using System;


namespace Org.Apache.Hadoop.Security.Authorize
{
	/// <summary>
	/// An abstract definition of <em>service</em> as related to
	/// Service Level Authorization for Hadoop.
	/// </summary>
	/// <remarks>
	/// An abstract definition of <em>service</em> as related to
	/// Service Level Authorization for Hadoop.
	/// Each service defines it's configuration key and also the necessary
	/// <see cref="Permission"/>
	/// required to access the service.
	/// </remarks>
	public class Service
	{
		private string key;

		private Type protocol;

		public Service(string key, Type protocol)
		{
			this.key = key;
			this.protocol = protocol;
		}

		/// <summary>Get the configuration key for the service.</summary>
		/// <returns>the configuration key for the service</returns>
		public virtual string GetServiceKey()
		{
			return key;
		}

		/// <summary>Get the protocol for the service</summary>
		/// <returns>
		/// the
		/// <see cref="System.Type{T}"/>
		/// for the protocol
		/// </returns>
		public virtual Type GetProtocol()
		{
			return protocol;
		}
	}
}
