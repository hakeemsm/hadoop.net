using System;


namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>Represent an RPC message as defined in RFC 1831.</summary>
	public abstract class RpcMessage
	{
		/// <summary>Message type</summary>
		[System.Serializable]
		public sealed class Type
		{
			public static readonly RpcMessage.Type RpcCall = new RpcMessage.Type();

			public static readonly RpcMessage.Type RpcReply = new RpcMessage.Type();

			// the order of the values below are significant.
			public int GetValue()
			{
				return Ordinal();
			}

			public static RpcMessage.Type FromValue(int value)
			{
				if (value < 0 || value >= Values().Length)
				{
					return null;
				}
				return Values()[value];
			}
		}

		protected internal readonly int xid;

		protected internal readonly RpcMessage.Type messageType;

		internal RpcMessage(int xid, RpcMessage.Type messageType)
		{
			if (messageType != RpcMessage.Type.RpcCall && messageType != RpcMessage.Type.RpcReply)
			{
				throw new ArgumentException("Invalid message type " + messageType);
			}
			this.xid = xid;
			this.messageType = messageType;
		}

		public abstract XDR Write(XDR xdr);

		public virtual int GetXid()
		{
			return xid;
		}

		public virtual RpcMessage.Type GetMessageType()
		{
			return messageType;
		}

		protected internal virtual void ValidateMessageType(RpcMessage.Type expected)
		{
			if (expected != messageType)
			{
				throw new ArgumentException("Message type is expected to be " + expected + " but got "
					 + messageType);
			}
		}
	}
}
