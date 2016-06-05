using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api
{
	/// <summary>
	/// NMTokenCache manages NMTokens required for an Application Master
	/// communicating with individual NodeManagers.
	/// </summary>
	/// <remarks>
	/// NMTokenCache manages NMTokens required for an Application Master
	/// communicating with individual NodeManagers.
	/// <p>
	/// By default Yarn client libraries
	/// <see cref="AMRMClient{T}"/>
	/// and
	/// <see cref="NMClient"/>
	/// use
	/// <see cref="GetSingleton()"/>
	/// instance of the cache.
	/// <ul>
	/// <li>
	/// Using the singleton instance of the cache is appropriate when running a
	/// single ApplicationMaster in the same JVM.
	/// </li>
	/// <li>
	/// When using the singleton, users don't need to do anything special,
	/// <see cref="AMRMClient{T}"/>
	/// and
	/// <see cref="NMClient"/>
	/// are already set up to use the
	/// default singleton
	/// <see cref="NMTokenCache"/>
	/// </li>
	/// </ul>
	/// If running multiple Application Masters in the same JVM, a different cache
	/// instance should be used for each Application Master.
	/// <ul>
	/// <li>
	/// If using the
	/// <see cref="AMRMClient{T}"/>
	/// and the
	/// <see cref="NMClient"/>
	/// , setting up
	/// and using an instance cache is as follows:
	/// <pre>
	/// NMTokenCache nmTokenCache = new NMTokenCache();
	/// AMRMClient rmClient = AMRMClient.createAMRMClient();
	/// NMClient nmClient = NMClient.createNMClient();
	/// nmClient.setNMTokenCache(nmTokenCache);
	/// ...
	/// </pre>
	/// </li>
	/// <li>
	/// If using the
	/// <see cref="Org.Apache.Hadoop.Yarn.Client.Api.Async.AMRMClientAsync{T}"/>
	/// and the
	/// <see cref="Org.Apache.Hadoop.Yarn.Client.Api.Async.NMClientAsync"/>
	/// ,
	/// setting up and using an instance cache is as follows:
	/// <pre>
	/// NMTokenCache nmTokenCache = new NMTokenCache();
	/// AMRMClient rmClient = AMRMClient.createAMRMClient();
	/// NMClient nmClient = NMClient.createNMClient();
	/// nmClient.setNMTokenCache(nmTokenCache);
	/// AMRMClientAsync rmClientAsync = new AMRMClientAsync(rmClient, 1000, [AMRM_CALLBACK]);
	/// NMClientAsync nmClientAsync = new NMClientAsync("nmClient", nmClient, [NM_CALLBACK]);
	/// ...
	/// </pre>
	/// </li>
	/// <li>
	/// If using
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol"/>
	/// and
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol"/>
	/// directly, setting up and using an
	/// instance cache is as follows:
	/// <pre>
	/// NMTokenCache nmTokenCache = new NMTokenCache();
	/// ...
	/// ApplicationMasterProtocol amPro = ClientRMProxy.createRMProxy(conf, ApplicationMasterProtocol.class);
	/// ...
	/// AllocateRequest allocateRequest = ...
	/// ...
	/// AllocateResponse allocateResponse = rmClient.allocate(allocateRequest);
	/// for (NMToken token : allocateResponse.getNMTokens()) {
	/// nmTokenCache.setToken(token.getNodeId().toString(), token.getToken());
	/// }
	/// ...
	/// ContainerManagementProtocolProxy nmPro = ContainerManagementProtocolProxy(conf, nmTokenCache);
	/// ...
	/// nmPro.startContainer(container, containerContext);
	/// ...
	/// </pre>
	/// </li>
	/// </ul>
	/// It is also possible to mix the usage of a client (
	/// <c>AMRMClient</c>
	/// or
	/// <c>NMClient</c>
	/// , or the async versions of them) with a protocol proxy
	/// (
	/// <c>ContainerManagementProtocolProxy</c>
	/// or
	/// <c>ApplicationMasterProtocol</c>
	/// ).
	/// </remarks>
	public class NMTokenCache
	{
		private static readonly Org.Apache.Hadoop.Yarn.Client.Api.NMTokenCache NmTokenCache
			 = new Org.Apache.Hadoop.Yarn.Client.Api.NMTokenCache();

		/// <summary>Returns the singleton NM token cache.</summary>
		/// <returns>the singleton NM token cache.</returns>
		public static Org.Apache.Hadoop.Yarn.Client.Api.NMTokenCache GetSingleton()
		{
			return NmTokenCache;
		}

		/// <summary>Returns NMToken, null if absent.</summary>
		/// <remarks>
		/// Returns NMToken, null if absent. Only the singleton obtained from
		/// <see cref="GetSingleton()"/>
		/// is looked at for the tokens. If you are using your
		/// own NMTokenCache that is different from the singleton, use
		/// <see cref="GetToken(string)"></see>
		/// </remarks>
		/// <param name="nodeAddr"/>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Token"/>
		/// NMToken required for communicating with node manager
		/// </returns>
		[InterfaceAudience.Public]
		public static Token GetNMToken(string nodeAddr)
		{
			return NmTokenCache.GetToken(nodeAddr);
		}

		/// <summary>
		/// Sets the NMToken for node address only in the singleton obtained from
		/// <see cref="GetSingleton()"/>
		/// . If you are using your own NMTokenCache that is
		/// different from the singleton, use
		/// <see cref="SetToken(string, Org.Apache.Hadoop.Yarn.Api.Records.Token)"></see>
		/// </summary>
		/// <param name="nodeAddr">node address (host:port)</param>
		/// <param name="token">NMToken</param>
		[InterfaceAudience.Public]
		public static void SetNMToken(string nodeAddr, Token token)
		{
			NmTokenCache.SetToken(nodeAddr, token);
		}

		private ConcurrentHashMap<string, Token> nmTokens;

		/// <summary>Creates a NM token cache instance.</summary>
		public NMTokenCache()
		{
			nmTokens = new ConcurrentHashMap<string, Token>();
		}

		/// <summary>Returns NMToken, null if absent</summary>
		/// <param name="nodeAddr"/>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Token"/>
		/// NMToken required for communicating with node
		/// manager
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public virtual Token GetToken(string nodeAddr)
		{
			return nmTokens[nodeAddr];
		}

		/// <summary>Sets the NMToken for node address</summary>
		/// <param name="nodeAddr">node address (host:port)</param>
		/// <param name="token">NMToken</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public virtual void SetToken(string nodeAddr, Token token)
		{
			nmTokens[nodeAddr] = token;
		}

		/// <summary>Returns true if NMToken is present in cache.</summary>
		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual bool ContainsToken(string nodeAddr)
		{
			return nmTokens.Contains(nodeAddr);
		}

		/// <summary>Returns the number of NMTokens present in cache.</summary>
		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual int NumberOfTokensInCache()
		{
			return nmTokens.Count;
		}

		/// <summary>Removes NMToken for specified node manager</summary>
		/// <param name="nodeAddr">node address (host:port)</param>
		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual void RemoveToken(string nodeAddr)
		{
			Sharpen.Collections.Remove(nmTokens, nodeAddr);
		}

		/// <summary>It will remove all the nm tokens from its cache</summary>
		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual void ClearCache()
		{
			nmTokens.Clear();
		}
	}
}
