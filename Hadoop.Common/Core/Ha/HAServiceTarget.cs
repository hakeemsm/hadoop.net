using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Collect;
using Javax.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA.ProtocolPB;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	/// <summary>Represents a target of the client side HA administration commands.</summary>
	public abstract class HAServiceTarget
	{
		private const string HostSubstKey = "host";

		private const string PortSubstKey = "port";

		private const string AddressSubstKey = "address";

		/// <returns>the IPC address of the target node.</returns>
		public abstract IPEndPoint GetAddress();

		/// <returns>the IPC address of the ZKFC on the target node</returns>
		public abstract IPEndPoint GetZKFCAddress();

		/// <returns>a Fencer implementation configured for this target node</returns>
		public abstract NodeFencer GetFencer();

		/// <exception cref="BadFencingConfigurationException">
		/// if the fencing configuration
		/// appears to be invalid. This is divorced from the above
		/// <see cref="GetFencer()"/>
		/// method so that the configuration can be checked
		/// during the pre-flight phase of failover.
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		public abstract void CheckFencingConfigured();

		/// <returns>a proxy to connect to the target HA Service.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual HAServiceProtocol GetProxy(Configuration conf, int timeoutMs)
		{
			Configuration confCopy = new Configuration(conf);
			// Lower the timeout so we quickly fail to connect
			confCopy.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesKey, 1);
			SocketFactory factory = NetUtils.GetDefaultSocketFactory(confCopy);
			return new HAServiceProtocolClientSideTranslatorPB(GetAddress(), confCopy, factory
				, timeoutMs);
		}

		/// <returns>a proxy to the ZKFC which is associated with this HA service.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual ZKFCProtocol GetZKFCProxy(Configuration conf, int timeoutMs)
		{
			Configuration confCopy = new Configuration(conf);
			// Lower the timeout so we quickly fail to connect
			confCopy.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesKey, 1);
			SocketFactory factory = NetUtils.GetDefaultSocketFactory(confCopy);
			return new ZKFCProtocolClientSideTranslatorPB(GetZKFCAddress(), confCopy, factory
				, timeoutMs);
		}

		public IDictionary<string, string> GetFencingParameters()
		{
			IDictionary<string, string> ret = Maps.NewHashMap();
			AddFencingParameters(ret);
			return ret;
		}

		/// <summary>
		/// Hook to allow subclasses to add any parameters they would like to
		/// expose to fencing implementations/scripts.
		/// </summary>
		/// <remarks>
		/// Hook to allow subclasses to add any parameters they would like to
		/// expose to fencing implementations/scripts. Fencing methods are free
		/// to use this map as they see fit -- notably, the shell script
		/// implementation takes each entry, prepends 'target_', substitutes
		/// '_' for '.', and adds it to the environment of the script.
		/// Subclass implementations should be sure to delegate to the superclass
		/// implementation as well as adding their own keys.
		/// </remarks>
		/// <param name="ret">map which can be mutated to pass parameters to the fencer</param>
		protected internal virtual void AddFencingParameters(IDictionary<string, string> 
			ret)
		{
			ret[AddressSubstKey] = GetAddress().ToString();
			ret[HostSubstKey] = GetAddress().GetHostName();
			ret[PortSubstKey] = GetAddress().Port.ToString();
		}

		/// <returns>true if auto failover should be considered enabled</returns>
		public virtual bool IsAutoFailoverEnabled()
		{
			return false;
		}
	}
}
