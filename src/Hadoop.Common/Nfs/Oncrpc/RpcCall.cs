using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>Represents an RPC message of type RPC call as defined in RFC 1831</summary>
	public class RpcCall : RpcMessage
	{
		public const int RpcVersion = 2;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Oncrpc.RpcCall
			));

		public static Org.Apache.Hadoop.Oncrpc.RpcCall Read(XDR xdr)
		{
			return new Org.Apache.Hadoop.Oncrpc.RpcCall(xdr.ReadInt(), RpcMessage.Type.FromValue
				(xdr.ReadInt()), xdr.ReadInt(), xdr.ReadInt(), xdr.ReadInt(), xdr.ReadInt(), Credentials
				.ReadFlavorAndCredentials(xdr), Verifier.ReadFlavorAndVerifier(xdr));
		}

		public static Org.Apache.Hadoop.Oncrpc.RpcCall GetInstance(int xid, int program, 
			int version, int procedure, Credentials cred, Verifier verifier)
		{
			return new Org.Apache.Hadoop.Oncrpc.RpcCall(xid, RpcMessage.Type.RpcCall, 2, program
				, version, procedure, cred, verifier);
		}

		private readonly int rpcVersion;

		private readonly int program;

		private readonly int version;

		private readonly int procedure;

		private readonly Credentials credentials;

		private readonly Verifier verifier;

		protected internal RpcCall(int xid, RpcMessage.Type messageType, int rpcVersion, 
			int program, int version, int procedure, Credentials credential, Verifier verifier
			)
			: base(xid, messageType)
		{
			this.rpcVersion = rpcVersion;
			this.program = program;
			this.version = version;
			this.procedure = procedure;
			this.credentials = credential;
			this.verifier = verifier;
			if (Log.IsTraceEnabled())
			{
				Log.Trace(this);
			}
			Validate();
		}

		private void ValidateRpcVersion()
		{
			if (rpcVersion != RpcVersion)
			{
				throw new ArgumentException("RPC version is expected to be " + RpcVersion + " but got "
					 + rpcVersion);
			}
		}

		public virtual void Validate()
		{
			ValidateMessageType(RpcMessage.Type.RpcCall);
			ValidateRpcVersion();
		}

		// Validate other members
		// Throw exception if validation fails
		public virtual int GetRpcVersion()
		{
			return rpcVersion;
		}

		public virtual int GetProgram()
		{
			return program;
		}

		public virtual int GetVersion()
		{
			return version;
		}

		public virtual int GetProcedure()
		{
			return procedure;
		}

		public virtual Credentials GetCredential()
		{
			return credentials;
		}

		public virtual Verifier GetVerifier()
		{
			return verifier;
		}

		public override XDR Write(XDR xdr)
		{
			xdr.WriteInt(xid);
			xdr.WriteInt(RpcMessage.Type.RpcCall.GetValue());
			xdr.WriteInt(2);
			xdr.WriteInt(program);
			xdr.WriteInt(version);
			xdr.WriteInt(procedure);
			Credentials.WriteFlavorAndCredentials(credentials, xdr);
			Verifier.WriteFlavorAndVerifier(verifier, xdr);
			return xdr;
		}

		public override string ToString()
		{
			return string.Format("Xid:%d, messageType:%s, rpcVersion:%d, program:%d," + " version:%d, procedure:%d, credential:%s, verifier:%s"
				, xid, messageType, rpcVersion, program, version, procedure, credentials.ToString
				(), verifier.ToString());
		}
	}
}
