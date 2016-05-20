using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>Represents a target of the client side HA administration commands.</summary>
	public abstract class HAServiceTarget
	{
		private const string HOST_SUBST_KEY = "host";

		private const string PORT_SUBST_KEY = "port";

		private const string ADDRESS_SUBST_KEY = "address";

		/// <returns>the IPC address of the target node.</returns>
		public abstract java.net.InetSocketAddress getAddress();

		/// <returns>the IPC address of the ZKFC on the target node</returns>
		public abstract java.net.InetSocketAddress getZKFCAddress();

		/// <returns>a Fencer implementation configured for this target node</returns>
		public abstract org.apache.hadoop.ha.NodeFencer getFencer();

		/// <exception cref="BadFencingConfigurationException">
		/// if the fencing configuration
		/// appears to be invalid. This is divorced from the above
		/// <see cref="getFencer()"/>
		/// method so that the configuration can be checked
		/// during the pre-flight phase of failover.
		/// </exception>
		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		public abstract void checkFencingConfigured();

		/// <returns>a proxy to connect to the target HA Service.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.ha.HAServiceProtocol getProxy(org.apache.hadoop.conf.Configuration
			 conf, int timeoutMs)
		{
			org.apache.hadoop.conf.Configuration confCopy = new org.apache.hadoop.conf.Configuration
				(conf);
			// Lower the timeout so we quickly fail to connect
			confCopy.setInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY
				, 1);
			javax.net.SocketFactory factory = org.apache.hadoop.net.NetUtils.getDefaultSocketFactory
				(confCopy);
			return new org.apache.hadoop.ha.protocolPB.HAServiceProtocolClientSideTranslatorPB
				(getAddress(), confCopy, factory, timeoutMs);
		}

		/// <returns>a proxy to the ZKFC which is associated with this HA service.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.ha.ZKFCProtocol getZKFCProxy(org.apache.hadoop.conf.Configuration
			 conf, int timeoutMs)
		{
			org.apache.hadoop.conf.Configuration confCopy = new org.apache.hadoop.conf.Configuration
				(conf);
			// Lower the timeout so we quickly fail to connect
			confCopy.setInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY
				, 1);
			javax.net.SocketFactory factory = org.apache.hadoop.net.NetUtils.getDefaultSocketFactory
				(confCopy);
			return new org.apache.hadoop.ha.protocolPB.ZKFCProtocolClientSideTranslatorPB(getZKFCAddress
				(), confCopy, factory, timeoutMs);
		}

		public System.Collections.Generic.IDictionary<string, string> getFencingParameters
			()
		{
			System.Collections.Generic.IDictionary<string, string> ret = com.google.common.collect.Maps
				.newHashMap();
			addFencingParameters(ret);
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
		protected internal virtual void addFencingParameters(System.Collections.Generic.IDictionary
			<string, string> ret)
		{
			ret[ADDRESS_SUBST_KEY] = Sharpen.Runtime.getStringValueOf(getAddress());
			ret[HOST_SUBST_KEY] = getAddress().getHostName();
			ret[PORT_SUBST_KEY] = Sharpen.Runtime.getStringValueOf(getAddress().getPort());
		}

		/// <returns>true if auto failover should be considered enabled</returns>
		public virtual bool isAutoFailoverEnabled()
		{
			return false;
		}
	}
}
