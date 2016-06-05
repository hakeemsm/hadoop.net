using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Oncrpc;


namespace Org.Apache.Hadoop.Oncrpc.Security
{
	public abstract class SecurityHandler
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(SecurityHandler));

		public abstract string GetUser();

		public abstract bool ShouldSilentlyDrop(RpcCall request);

		/// <exception cref="System.IO.IOException"/>
		public abstract Verifier GetVerifer(RpcCall request);

		public virtual bool IsUnwrapRequired()
		{
			return false;
		}

		public virtual bool IsWrapRequired()
		{
			return false;
		}

		/// <summary>Used by GSS</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual XDR Unwrap(RpcCall request, byte[] data)
		{
			throw new NotSupportedException();
		}

		/// <summary>Used by GSS</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual byte[] Wrap(RpcCall request, XDR response)
		{
			throw new NotSupportedException();
		}

		/// <summary>Used by AUTH_SYS</summary>
		public virtual int GetUid()
		{
			throw new NotSupportedException();
		}

		/// <summary>Used by AUTH_SYS</summary>
		public virtual int GetGid()
		{
			throw new NotSupportedException();
		}

		/// <summary>Used by AUTH_SYS</summary>
		public virtual int[] GetAuxGids()
		{
			throw new NotSupportedException();
		}
	}
}
