using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// A class defining a set of static helper methods to provide conversion between
	/// bytes and string for UUID-based client Id.
	/// </summary>
	public class ClientId
	{
		/// <summary>The byte array of a UUID should be 16</summary>
		public const int BYTE_LENGTH = 16;

		private const int shiftWidth = 8;

		/// <summary>Return clientId as byte[]</summary>
		public static byte[] getClientId()
		{
			java.util.UUID uuid = java.util.UUID.randomUUID();
			java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(new byte[BYTE_LENGTH]);
			buf.putLong(uuid.getMostSignificantBits());
			buf.putLong(uuid.getLeastSignificantBits());
			return ((byte[])buf.array());
		}

		/// <summary>Convert a clientId byte[] to string</summary>
		public static string toString(byte[] clientId)
		{
			// clientId can be null or an empty array
			if (clientId == null || clientId.Length == 0)
			{
				return string.Empty;
			}
			// otherwise should be 16 bytes
			com.google.common.@base.Preconditions.checkArgument(clientId.Length == BYTE_LENGTH
				);
			long msb = getMsb(clientId);
			long lsb = getLsb(clientId);
			return (new java.util.UUID(msb, lsb)).ToString();
		}

		public static long getMsb(byte[] clientId)
		{
			long msb = 0;
			for (int i = 0; i < BYTE_LENGTH / 2; i++)
			{
				msb = (msb << shiftWidth) | (clientId[i] & unchecked((int)(0xff)));
			}
			return msb;
		}

		public static long getLsb(byte[] clientId)
		{
			long lsb = 0;
			for (int i = BYTE_LENGTH / 2; i < BYTE_LENGTH; i++)
			{
				lsb = (lsb << shiftWidth) | (clientId[i] & unchecked((int)(0xff)));
			}
			return lsb;
		}

		/// <summary>Convert from clientId string byte[] representation of clientId</summary>
		public static byte[] toBytes(string id)
		{
			if (id == null || string.Empty.Equals(id))
			{
				return new byte[0];
			}
			java.util.UUID uuid = java.util.UUID.fromString(id);
			java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(new byte[BYTE_LENGTH]);
			buf.putLong(uuid.getMostSignificantBits());
			buf.putLong(uuid.getLeastSignificantBits());
			return ((byte[])buf.array());
		}
	}
}
