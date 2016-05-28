using System;
using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer
{
	/// <summary>Class used to indicate whether a channel is trusted or not.</summary>
	/// <remarks>
	/// Class used to indicate whether a channel is trusted or not.
	/// The default implementation is to return false indicating that
	/// the channel is not trusted.
	/// This class can be overridden to provide custom logic to determine
	/// whether a channel is trusted or not.
	/// The custom class can be specified via configuration.
	/// </remarks>
	public class TrustedChannelResolver : Configurable
	{
		internal Configuration conf;

		/// <summary>Returns an instance of TrustedChannelResolver.</summary>
		/// <remarks>
		/// Returns an instance of TrustedChannelResolver.
		/// Looks up the configuration to see if there is custom class specified.
		/// </remarks>
		/// <param name="conf"/>
		/// <returns>TrustedChannelResolver</returns>
		public static TrustedChannelResolver GetInstance(Configuration conf)
		{
			Type clazz = conf.GetClass<TrustedChannelResolver>(DFSConfigKeys.DfsTrustedchannelResolverClass
				, typeof(TrustedChannelResolver));
			return ReflectionUtils.NewInstance(clazz, conf);
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <summary>
		/// Return boolean value indicating whether a channel is trusted or not
		/// from a client's perspective.
		/// </summary>
		/// <returns>true if the channel is trusted and false otherwise.</returns>
		public virtual bool IsTrusted()
		{
			return false;
		}

		/// <summary>Identify boolean value indicating whether a channel is trusted or not.</summary>
		/// <param name="peerAddress">address of the peer</param>
		/// <returns>true if the channel is trusted and false otherwise.</returns>
		public virtual bool IsTrusted(IPAddress peerAddress)
		{
			return false;
		}
	}
}
