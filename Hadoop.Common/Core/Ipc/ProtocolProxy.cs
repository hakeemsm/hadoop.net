using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Security;


namespace Org.Apache.Hadoop.Ipc
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
		private Type protocol;

		private T proxy;

		private HashSet<int> serverMethods = null;

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
		public ProtocolProxy(Type protocol, T proxy, bool supportServerMethodCheck)
		{
			this.protocol = protocol;
			this.proxy = proxy;
			this.supportServerMethodCheck = supportServerMethodCheck;
		}

		/// <exception cref="System.IO.IOException"/>
		private void FetchServerMethods(MethodInfo method)
		{
			long clientVersion;
			clientVersion = RPC.GetProtocolVersion(method.DeclaringType);
			int clientMethodsHash = ProtocolSignature.GetFingerprint(method.DeclaringType.GetMethods
				());
			ProtocolSignature serverInfo = ((VersionedProtocol)proxy).GetProtocolSignature(RPC
				.GetProtocolName(protocol), clientVersion, clientMethodsHash);
			long serverVersion = serverInfo.GetVersion();
			if (serverVersion != clientVersion)
			{
				throw new RPC.VersionMismatch(protocol.FullName, clientVersion, serverVersion);
			}
			int[] serverMethodsCodes = serverInfo.GetMethods();
			if (serverMethodsCodes != null)
			{
				serverMethods = new HashSet<int>(serverMethodsCodes.Length);
				foreach (int m in serverMethodsCodes)
				{
					this.serverMethods.AddItem(Extensions.ValueOf(m));
				}
			}
			serverMethodsFetched = true;
		}

		/*
		* Get the proxy
		*/
		public virtual T GetProxy()
		{
			return proxy;
		}

		/// <summary>Check if a method is supported by the server or not</summary>
		/// <param name="methodName">a method's name in String format</param>
		/// <param name="parameterTypes">a method's parameter types</param>
		/// <returns>true if the method is supported by the server</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsMethodSupported(string methodName, params Type[] parameterTypes
			)
		{
			lock (this)
			{
				if (!supportServerMethodCheck)
				{
					return true;
				}
				MethodInfo method;
				try
				{
					method = Runtime.GetDeclaredMethod(protocol, methodName, parameterTypes);
				}
				catch (SecurityException e)
				{
					throw new IOException(e);
				}
				catch (MissingMethodException e)
				{
					throw new IOException(e);
				}
				if (!serverMethodsFetched)
				{
					FetchServerMethods(method);
				}
				if (serverMethods == null)
				{
					// client & server have the same protocol
					return true;
				}
				return serverMethods.Contains(Extensions.ValueOf(ProtocolSignature.GetFingerprint
					(method)));
			}
		}
	}
}
