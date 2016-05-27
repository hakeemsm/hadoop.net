using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	public class ProtocolSignature : Writable
	{
		static ProtocolSignature()
		{
			// register a ctor
			WritableFactories.SetFactory(typeof(Org.Apache.Hadoop.Ipc.ProtocolSignature), new 
				_WritableFactory_38());
		}

		private sealed class _WritableFactory_38 : WritableFactory
		{
			public _WritableFactory_38()
			{
			}

			public Writable NewInstance()
			{
				return new Org.Apache.Hadoop.Ipc.ProtocolSignature();
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

		public virtual long GetVersion()
		{
			return version;
		}

		public virtual int[] GetMethods()
		{
			return methods;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			version = @in.ReadLong();
			bool hasMethods = @in.ReadBoolean();
			if (hasMethods)
			{
				int numMethods = @in.ReadInt();
				methods = new int[numMethods];
				for (int i = 0; i < numMethods; i++)
				{
					methods[i] = @in.ReadInt();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteLong(version);
			if (methods == null)
			{
				@out.WriteBoolean(false);
			}
			else
			{
				@out.WriteBoolean(true);
				@out.WriteInt(methods.Length);
				foreach (int method in methods)
				{
					@out.WriteInt(method);
				}
			}
		}

		/// <summary>
		/// Calculate a method's hash code considering its method
		/// name, returning type, and its parameter types
		/// </summary>
		/// <param name="method">a method</param>
		/// <returns>its hash code</returns>
		internal static int GetFingerprint(MethodInfo method)
		{
			int hashcode = method.Name.GetHashCode();
			hashcode = hashcode + 31 * method.ReturnType.FullName.GetHashCode();
			foreach (Type type in Sharpen.Runtime.GetParameterTypes(method))
			{
				hashcode = 31 * hashcode ^ type.FullName.GetHashCode();
			}
			return hashcode;
		}

		/// <summary>Convert an array of Method into an array of hash codes</summary>
		/// <param name="methods"/>
		/// <returns>array of hash codes</returns>
		private static int[] GetFingerprints(MethodInfo[] methods)
		{
			if (methods == null)
			{
				return null;
			}
			int[] hashCodes = new int[methods.Length];
			for (int i = 0; i < methods.Length; i++)
			{
				hashCodes[i] = GetFingerprint(methods[i]);
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
		internal static int GetFingerprint(MethodInfo[] methods)
		{
			return GetFingerprint(GetFingerprints(methods));
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
		internal static int GetFingerprint(int[] hashcodes)
		{
			Arrays.Sort(hashcodes);
			return Arrays.HashCode(hashcodes);
		}

		private class ProtocolSigFingerprint
		{
			private ProtocolSignature signature;

			private int fingerprint;

			internal ProtocolSigFingerprint(ProtocolSignature sig, int fingerprint)
			{
				this.signature = sig;
				this.fingerprint = fingerprint;
			}
		}

		/// <summary>A cache that maps a protocol's name to its signature & finger print</summary>
		private static readonly Dictionary<string, ProtocolSignature.ProtocolSigFingerprint
			> ProtocolFingerprintCache = new Dictionary<string, ProtocolSignature.ProtocolSigFingerprint
			>();

		[VisibleForTesting]
		public static void ResetCache()
		{
			ProtocolFingerprintCache.Clear();
		}

		/// <summary>Return a protocol's signature and finger print from cache</summary>
		/// <param name="protocol">a protocol class</param>
		/// <param name="serverVersion">protocol version</param>
		/// <returns>its signature and finger print</returns>
		private static ProtocolSignature.ProtocolSigFingerprint GetSigFingerprint(Type protocol
			, long serverVersion)
		{
			string protocolName = RPC.GetProtocolName(protocol);
			lock (ProtocolFingerprintCache)
			{
				ProtocolSignature.ProtocolSigFingerprint sig = ProtocolFingerprintCache[protocolName
					];
				if (sig == null)
				{
					int[] serverMethodHashcodes = GetFingerprints(protocol.GetMethods());
					sig = new ProtocolSignature.ProtocolSigFingerprint(new ProtocolSignature(serverVersion
						, serverMethodHashcodes), GetFingerprint(serverMethodHashcodes));
					ProtocolFingerprintCache[protocolName] = sig;
				}
				return sig;
			}
		}

		/// <summary>Get a server protocol's signature</summary>
		/// <param name="clientMethodsHashCode">client protocol methods hashcode</param>
		/// <param name="serverVersion">server protocol version</param>
		/// <param name="protocol">protocol</param>
		/// <returns>the server's protocol signature</returns>
		public static ProtocolSignature GetProtocolSignature(int clientMethodsHashCode, long
			 serverVersion, Type protocol)
		{
			// try to get the finger print & signature from the cache
			ProtocolSignature.ProtocolSigFingerprint sig = GetSigFingerprint(protocol, serverVersion
				);
			// check if the client side protocol matches the one on the server side
			if (clientMethodsHashCode == sig.fingerprint)
			{
				return new ProtocolSignature(serverVersion, null);
			}
			// null indicates a match
			return sig.signature;
		}

		/// <exception cref="System.TypeLoadException"/>
		public static ProtocolSignature GetProtocolSignature(string protocolName, long version
			)
		{
			Type protocol = Sharpen.Runtime.GetType(protocolName);
			return GetSigFingerprint(protocol, version).signature;
		}

		/// <summary>Get a server protocol's signature</summary>
		/// <param name="server">server implementation</param>
		/// <param name="protocol">server protocol</param>
		/// <param name="clientVersion">client's version</param>
		/// <param name="clientMethodsHash">client's protocol's hash code</param>
		/// <returns>the server protocol's signature</returns>
		/// <exception cref="System.IO.IOException">if any error occurs</exception>
		public static ProtocolSignature GetProtocolSignature(VersionedProtocol server, string
			 protocol, long clientVersion, int clientMethodsHash)
		{
			Type inter;
			try
			{
				inter = (Type)Sharpen.Runtime.GetType(protocol);
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
			long serverVersion = server.GetProtocolVersion(protocol, clientVersion);
			return ProtocolSignature.GetProtocolSignature(clientMethodsHash, serverVersion, inter
				);
		}
	}
}
