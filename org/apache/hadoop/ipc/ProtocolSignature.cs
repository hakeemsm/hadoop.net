using Sharpen;

namespace org.apache.hadoop.ipc
{
	public class ProtocolSignature : org.apache.hadoop.io.Writable
	{
		static ProtocolSignature()
		{
			// register a ctor
			org.apache.hadoop.io.WritableFactories.setFactory(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.ProtocolSignature)), new _WritableFactory_38());
		}

		private sealed class _WritableFactory_38 : org.apache.hadoop.io.WritableFactory
		{
			public _WritableFactory_38()
			{
			}

			public org.apache.hadoop.io.Writable newInstance()
			{
				return new org.apache.hadoop.ipc.ProtocolSignature();
			}
		}

		private long version;

		private int[] methods = null;

		/// <summary>default constructor</summary>
		public ProtocolSignature()
		{
		}

		/// <summary>Constructor</summary>
		/// <param name="version">server version</param>
		/// <param name="methodHashcodes">hash codes of the methods supported by server</param>
		public ProtocolSignature(long version, int[] methodHashcodes)
		{
			// an array of method hash codes
			this.version = version;
			this.methods = methodHashcodes;
		}

		public virtual long getVersion()
		{
			return version;
		}

		public virtual int[] getMethods()
		{
			return methods;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			version = @in.readLong();
			bool hasMethods = @in.readBoolean();
			if (hasMethods)
			{
				int numMethods = @in.readInt();
				methods = new int[numMethods];
				for (int i = 0; i < numMethods; i++)
				{
					methods[i] = @in.readInt();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeLong(version);
			if (methods == null)
			{
				@out.writeBoolean(false);
			}
			else
			{
				@out.writeBoolean(true);
				@out.writeInt(methods.Length);
				foreach (int method in methods)
				{
					@out.writeInt(method);
				}
			}
		}

		/// <summary>
		/// Calculate a method's hash code considering its method
		/// name, returning type, and its parameter types
		/// </summary>
		/// <param name="method">a method</param>
		/// <returns>its hash code</returns>
		internal static int getFingerprint(java.lang.reflect.Method method)
		{
			int hashcode = method.getName().GetHashCode();
			hashcode = hashcode + 31 * method.getReturnType().getName().GetHashCode();
			foreach (java.lang.Class type in method.getParameterTypes())
			{
				hashcode = 31 * hashcode ^ type.getName().GetHashCode();
			}
			return hashcode;
		}

		/// <summary>Convert an array of Method into an array of hash codes</summary>
		/// <param name="methods"/>
		/// <returns>array of hash codes</returns>
		private static int[] getFingerprints(java.lang.reflect.Method[] methods)
		{
			if (methods == null)
			{
				return null;
			}
			int[] hashCodes = new int[methods.Length];
			for (int i = 0; i < methods.Length; i++)
			{
				hashCodes[i] = getFingerprint(methods[i]);
			}
			return hashCodes;
		}

		/// <summary>
		/// Get the hash code of an array of methods
		/// Methods are sorted before hashcode is calculated.
		/// </summary>
		/// <remarks>
		/// Get the hash code of an array of methods
		/// Methods are sorted before hashcode is calculated.
		/// So the returned value is irrelevant of the method order in the array.
		/// </remarks>
		/// <param name="methods">an array of methods</param>
		/// <returns>the hash code</returns>
		internal static int getFingerprint(java.lang.reflect.Method[] methods)
		{
			return getFingerprint(getFingerprints(methods));
		}

		/// <summary>
		/// Get the hash code of an array of hashcodes
		/// Hashcodes are sorted before hashcode is calculated.
		/// </summary>
		/// <remarks>
		/// Get the hash code of an array of hashcodes
		/// Hashcodes are sorted before hashcode is calculated.
		/// So the returned value is irrelevant of the hashcode order in the array.
		/// </remarks>
		/// <param name="methods">an array of methods</param>
		/// <returns>the hash code</returns>
		internal static int getFingerprint(int[] hashcodes)
		{
			java.util.Arrays.sort(hashcodes);
			return java.util.Arrays.hashCode(hashcodes);
		}

		private class ProtocolSigFingerprint
		{
			private org.apache.hadoop.ipc.ProtocolSignature signature;

			private int fingerprint;

			internal ProtocolSigFingerprint(org.apache.hadoop.ipc.ProtocolSignature sig, int 
				fingerprint)
			{
				this.signature = sig;
				this.fingerprint = fingerprint;
			}
		}

		/// <summary>A cache that maps a protocol's name to its signature & finger print</summary>
		private static readonly System.Collections.Generic.Dictionary<string, org.apache.hadoop.ipc.ProtocolSignature.ProtocolSigFingerprint
			> PROTOCOL_FINGERPRINT_CACHE = new System.Collections.Generic.Dictionary<string, 
			org.apache.hadoop.ipc.ProtocolSignature.ProtocolSigFingerprint>();

		[com.google.common.annotations.VisibleForTesting]
		public static void resetCache()
		{
			PROTOCOL_FINGERPRINT_CACHE.clear();
		}

		/// <summary>Return a protocol's signature and finger print from cache</summary>
		/// <param name="protocol">a protocol class</param>
		/// <param name="serverVersion">protocol version</param>
		/// <returns>its signature and finger print</returns>
		private static org.apache.hadoop.ipc.ProtocolSignature.ProtocolSigFingerprint getSigFingerprint
			(java.lang.Class protocol, long serverVersion)
		{
			string protocolName = org.apache.hadoop.ipc.RPC.getProtocolName(protocol);
			lock (PROTOCOL_FINGERPRINT_CACHE)
			{
				org.apache.hadoop.ipc.ProtocolSignature.ProtocolSigFingerprint sig = PROTOCOL_FINGERPRINT_CACHE
					[protocolName];
				if (sig == null)
				{
					int[] serverMethodHashcodes = getFingerprints(protocol.getMethods());
					sig = new org.apache.hadoop.ipc.ProtocolSignature.ProtocolSigFingerprint(new org.apache.hadoop.ipc.ProtocolSignature
						(serverVersion, serverMethodHashcodes), getFingerprint(serverMethodHashcodes));
					PROTOCOL_FINGERPRINT_CACHE[protocolName] = sig;
				}
				return sig;
			}
		}

		/// <summary>Get a server protocol's signature</summary>
		/// <param name="clientMethodsHashCode">client protocol methods hashcode</param>
		/// <param name="serverVersion">server protocol version</param>
		/// <param name="protocol">protocol</param>
		/// <returns>the server's protocol signature</returns>
		public static org.apache.hadoop.ipc.ProtocolSignature getProtocolSignature(int clientMethodsHashCode
			, long serverVersion, java.lang.Class protocol)
		{
			// try to get the finger print & signature from the cache
			org.apache.hadoop.ipc.ProtocolSignature.ProtocolSigFingerprint sig = getSigFingerprint
				(protocol, serverVersion);
			// check if the client side protocol matches the one on the server side
			if (clientMethodsHashCode == sig.fingerprint)
			{
				return new org.apache.hadoop.ipc.ProtocolSignature(serverVersion, null);
			}
			// null indicates a match
			return sig.signature;
		}

		/// <exception cref="java.lang.ClassNotFoundException"/>
		public static org.apache.hadoop.ipc.ProtocolSignature getProtocolSignature(string
			 protocolName, long version)
		{
			java.lang.Class protocol = java.lang.Class.forName(protocolName);
			return getSigFingerprint(protocol, version).signature;
		}

		/// <summary>Get a server protocol's signature</summary>
		/// <param name="server">server implementation</param>
		/// <param name="protocol">server protocol</param>
		/// <param name="clientVersion">client's version</param>
		/// <param name="clientMethodsHash">client's protocol's hash code</param>
		/// <returns>the server protocol's signature</returns>
		/// <exception cref="System.IO.IOException">if any error occurs</exception>
		public static org.apache.hadoop.ipc.ProtocolSignature getProtocolSignature(org.apache.hadoop.ipc.VersionedProtocol
			 server, string protocol, long clientVersion, int clientMethodsHash)
		{
			java.lang.Class inter;
			try
			{
				inter = (java.lang.Class)java.lang.Class.forName(protocol);
			}
			catch (System.Exception e)
			{
				throw new System.IO.IOException(e);
			}
			long serverVersion = server.getProtocolVersion(protocol, clientVersion);
			return org.apache.hadoop.ipc.ProtocolSignature.getProtocolSignature(clientMethodsHash
				, serverVersion, inter);
		}
	}
}
