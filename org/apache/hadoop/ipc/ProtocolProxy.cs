using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// a class wraps around a server's proxy,
	/// containing a list of its supported methods.
	/// </summary>
	/// <remarks>
	/// a class wraps around a server's proxy,
	/// containing a list of its supported methods.
	/// A list of methods with a value of null indicates that the client and server
	/// have the same protocol.
	/// </remarks>
	public class ProtocolProxy<T>
	{
		private java.lang.Class protocol;

		private T proxy;

		private java.util.HashSet<int> serverMethods = null;

		private readonly bool supportServerMethodCheck;

		private bool serverMethodsFetched = false;

		/// <summary>Constructor</summary>
		/// <param name="protocol">protocol class</param>
		/// <param name="proxy">its proxy</param>
		/// <param name="supportServerMethodCheck">
		/// If false proxy will never fetch server
		/// methods and isMethodSupported will always return true. If true,
		/// server methods will be fetched for the first call to
		/// isMethodSupported.
		/// </param>
		public ProtocolProxy(java.lang.Class protocol, T proxy, bool supportServerMethodCheck
			)
		{
			this.protocol = protocol;
			this.proxy = proxy;
			this.supportServerMethodCheck = supportServerMethodCheck;
		}

		/// <exception cref="System.IO.IOException"/>
		private void fetchServerMethods(java.lang.reflect.Method method)
		{
			long clientVersion;
			clientVersion = org.apache.hadoop.ipc.RPC.getProtocolVersion(method.getDeclaringClass
				());
			int clientMethodsHash = org.apache.hadoop.ipc.ProtocolSignature.getFingerprint(method
				.getDeclaringClass().getMethods());
			org.apache.hadoop.ipc.ProtocolSignature serverInfo = ((org.apache.hadoop.ipc.VersionedProtocol
				)proxy).getProtocolSignature(org.apache.hadoop.ipc.RPC.getProtocolName(protocol)
				, clientVersion, clientMethodsHash);
			long serverVersion = serverInfo.getVersion();
			if (serverVersion != clientVersion)
			{
				throw new org.apache.hadoop.ipc.RPC.VersionMismatch(protocol.getName(), clientVersion
					, serverVersion);
			}
			int[] serverMethodsCodes = serverInfo.getMethods();
			if (serverMethodsCodes != null)
			{
				serverMethods = new java.util.HashSet<int>(serverMethodsCodes.Length);
				foreach (int m in serverMethodsCodes)
				{
					this.serverMethods.add(int.Parse(m));
				}
			}
			serverMethodsFetched = true;
		}

		/*
		* Get the proxy
		*/
		public virtual T getProxy()
		{
			return proxy;
		}

		/// <summary>Check if a method is supported by the server or not</summary>
		/// <param name="methodName">a method's name in String format</param>
		/// <param name="parameterTypes">a method's parameter types</param>
		/// <returns>true if the method is supported by the server</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool isMethodSupported(string methodName, params java.lang.Class[]
			 parameterTypes)
		{
			lock (this)
			{
				if (!supportServerMethodCheck)
				{
					return true;
				}
				java.lang.reflect.Method method;
				try
				{
					method = protocol.getDeclaredMethod(methodName, parameterTypes);
				}
				catch (System.Security.SecurityException e)
				{
					throw new System.IO.IOException(e);
				}
				catch (System.MissingMethodException e)
				{
					throw new System.IO.IOException(e);
				}
				if (!serverMethodsFetched)
				{
					fetchServerMethods(method);
				}
				if (serverMethods == null)
				{
					// client & server have the same protocol
					return true;
				}
				return serverMethods.contains(int.Parse(org.apache.hadoop.ipc.ProtocolSignature.getFingerprint
					(method)));
			}
		}
	}
}
