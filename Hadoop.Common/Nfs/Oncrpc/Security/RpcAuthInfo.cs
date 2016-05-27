using System;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc.Security
{
	/// <summary>Authentication Info.</summary>
	/// <remarks>Authentication Info. Base class of Verifier and Credential.</remarks>
	public abstract class RpcAuthInfo
	{
		/// <summary>Different types of authentication as defined in RFC 1831</summary>
		[System.Serializable]
		public sealed class AuthFlavor
		{
			public static readonly RpcAuthInfo.AuthFlavor AuthNone = new RpcAuthInfo.AuthFlavor
				(0);

			public static readonly RpcAuthInfo.AuthFlavor AuthSys = new RpcAuthInfo.AuthFlavor
				(1);

			public static readonly RpcAuthInfo.AuthFlavor AuthShort = new RpcAuthInfo.AuthFlavor
				(2);

			public static readonly RpcAuthInfo.AuthFlavor AuthDh = new RpcAuthInfo.AuthFlavor
				(3);

			public static readonly RpcAuthInfo.AuthFlavor RpcsecGss = new RpcAuthInfo.AuthFlavor
				(6);

			private int value;

			internal AuthFlavor(int value)
			{
				this.value = value;
			}

			public int GetValue()
			{
				return RpcAuthInfo.AuthFlavor.value;
			}

			internal static RpcAuthInfo.AuthFlavor FromValue(int value)
			{
				foreach (RpcAuthInfo.AuthFlavor v in Values())
				{
					if (v.value == value)
					{
						return v;
					}
				}
				throw new ArgumentException("Invalid AuthFlavor value " + value);
			}
		}

		private readonly RpcAuthInfo.AuthFlavor flavor;

		protected internal RpcAuthInfo(RpcAuthInfo.AuthFlavor flavor)
		{
			this.flavor = flavor;
		}

		/// <summary>Load auth info</summary>
		public abstract void Read(XDR xdr);

		/// <summary>Write auth info</summary>
		public abstract void Write(XDR xdr);

		public virtual RpcAuthInfo.AuthFlavor GetFlavor()
		{
			return flavor;
		}

		public override string ToString()
		{
			return "(AuthFlavor:" + flavor + ")";
		}
	}
}
