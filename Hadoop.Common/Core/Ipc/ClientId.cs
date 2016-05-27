using Com.Google.Common.Base;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>
	/// A class defining a set of static helper methods to provide conversion between
	/// bytes and string for UUID-based client Id.
	/// </summary>
	public class ClientId
	{
		/// <summary>The byte array of a UUID should be 16</summary>
		public const int ByteLength = 16;

		private const int shiftWidth = 8;

		/// <summary>Return clientId as byte[]</summary>
		public static byte[] GetClientId()
		{
			UUID uuid = UUID.RandomUUID();
			ByteBuffer buf = ByteBuffer.Wrap(new byte[ByteLength]);
			buf.PutLong(uuid.GetMostSignificantBits());
			buf.PutLong(uuid.GetLeastSignificantBits());
			return ((byte[])buf.Array());
		}

		/// <summary>Convert a clientId byte[] to string</summary>
		public static string ToString(byte[] clientId)
		{
			// clientId can be null or an empty array
			if (clientId == null || clientId.Length == 0)
			{
				return string.Empty;
			}
			// otherwise should be 16 bytes
			Preconditions.CheckArgument(clientId.Length == ByteLength);
			long msb = GetMsb(clientId);
			long lsb = GetLsb(clientId);
			return (new UUID(msb, lsb)).ToString();
		}

		public static long GetMsb(byte[] clientId)
		{
			long msb = 0;
			for (int i = 0; i < ByteLength / 2; i++)
			{
				msb = (msb << shiftWidth) | (clientId[i] & unchecked((int)(0xff)));
			}
			return msb;
		}

		public static long GetLsb(byte[] clientId)
		{
			long lsb = 0;
			for (int i = ByteLength / 2; i < ByteLength; i++)
			{
				lsb = (lsb << shiftWidth) | (clientId[i] & unchecked((int)(0xff)));
			}
			return lsb;
		}

		/// <summary>Convert from clientId string byte[] representation of clientId</summary>
		public static byte[] ToBytes(string id)
		{
			if (id == null || string.Empty.Equals(id))
			{
				return new byte[0];
			}
			UUID uuid = UUID.FromString(id);
			ByteBuffer buf = ByteBuffer.Wrap(new byte[ByteLength]);
			buf.PutLong(uuid.GetMostSignificantBits());
			buf.PutLong(uuid.GetLeastSignificantBits());
			return ((byte[])buf.Array());
		}
	}
}
