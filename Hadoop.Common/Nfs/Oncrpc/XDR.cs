using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.IO;
using Org.Jboss.Netty.Buffer;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>Utility class for building XDR messages based on RFC 4506.</summary>
	/// <remarks>
	/// Utility class for building XDR messages based on RFC 4506.
	/// Key points of the format:
	/// <ul>
	/// <li>Primitives are stored in big-endian order (i.e., the default byte order
	/// of ByteBuffer).</li>
	/// <li>Booleans are stored as an integer.</li>
	/// <li>Each field in the message is always aligned by 4.</li>
	/// </ul>
	/// </remarks>
	public sealed class XDR
	{
		private const int DefaultInitialCapacity = 256;

		private const int SizeofInt = 4;

		private const int SizeofLong = 8;

		private static readonly byte[] PaddingBytes = new byte[] { 0, 0, 0, 0 };

		private ByteBuffer buf;

		public enum State
		{
			Reading,
			Writing
		}

		private readonly XDR.State state;

		/// <summary>Construct a new XDR message buffer.</summary>
		/// <param name="initialCapacity">the initial capacity of the buffer.</param>
		public XDR(int initialCapacity)
			: this(ByteBuffer.Allocate(initialCapacity), XDR.State.Writing)
		{
		}

		public XDR()
			: this(DefaultInitialCapacity)
		{
		}

		public XDR(ByteBuffer buf, XDR.State state)
		{
			this.buf = buf;
			this.state = state;
		}

		/// <summary>Wraps a byte array as a read-only XDR message.</summary>
		/// <remarks>
		/// Wraps a byte array as a read-only XDR message. There's no copy involved,
		/// thus it is the client's responsibility to ensure that the byte array
		/// remains unmodified when using the XDR object.
		/// </remarks>
		/// <param name="src">the byte array to be wrapped.</param>
		public XDR(byte[] src)
			: this(ByteBuffer.Wrap(src).AsReadOnlyBuffer(), XDR.State.Reading)
		{
		}

		public XDR AsReadOnlyWrap()
		{
			ByteBuffer b = buf.AsReadOnlyBuffer();
			if (state == XDR.State.Writing)
			{
				b.Flip();
			}
			XDR n = new XDR(b, XDR.State.Reading);
			return n;
		}

		public ByteBuffer Buffer()
		{
			return buf.Duplicate();
		}

		public int Size()
		{
			// TODO: This overloading intends to be compatible with the semantics of
			// the previous version of the class. This function should be separated into
			// two with clear semantics.
			return state == XDR.State.Reading ? buf.Limit() : buf.Position();
		}

		public int ReadInt()
		{
			Preconditions.CheckState(state == XDR.State.Reading);
			return buf.GetInt();
		}

		public void WriteInt(int v)
		{
			EnsureFreeSpace(SizeofInt);
			buf.PutInt(v);
		}

		public bool ReadBoolean()
		{
			Preconditions.CheckState(state == XDR.State.Reading);
			return buf.GetInt() != 0;
		}

		public void WriteBoolean(bool v)
		{
			EnsureFreeSpace(SizeofInt);
			buf.PutInt(v ? 1 : 0);
		}

		public long ReadHyper()
		{
			Preconditions.CheckState(state == XDR.State.Reading);
			return buf.GetLong();
		}

		public void WriteLongAsHyper(long v)
		{
			EnsureFreeSpace(SizeofLong);
			buf.PutLong(v);
		}

		public byte[] ReadFixedOpaque(int size)
		{
			Preconditions.CheckState(state == XDR.State.Reading);
			byte[] r = new byte[size];
			buf.Get(r);
			AlignPosition();
			return r;
		}

		public void WriteFixedOpaque(byte[] src, int length)
		{
			EnsureFreeSpace(AlignUp(length));
			buf.Put(src, 0, length);
			WritePadding();
		}

		public void WriteFixedOpaque(byte[] src)
		{
			WriteFixedOpaque(src, src.Length);
		}

		public byte[] ReadVariableOpaque()
		{
			Preconditions.CheckState(state == XDR.State.Reading);
			int size = ReadInt();
			return ReadFixedOpaque(size);
		}

		public void WriteVariableOpaque(byte[] src)
		{
			EnsureFreeSpace(SizeofInt + AlignUp(src.Length));
			buf.PutInt(src.Length);
			WriteFixedOpaque(src);
		}

		public string ReadString()
		{
			return new string(ReadVariableOpaque(), Charsets.Utf8);
		}

		public void WriteString(string s)
		{
			WriteVariableOpaque(Sharpen.Runtime.GetBytesForString(s, Charsets.Utf8));
		}

		private void WritePadding()
		{
			Preconditions.CheckState(state == XDR.State.Writing);
			int p = Pad(buf.Position());
			EnsureFreeSpace(p);
			buf.Put(PaddingBytes, 0, p);
		}

		private int AlignUp(int length)
		{
			return length + Pad(length);
		}

		private int Pad(int length)
		{
			switch (length % 4)
			{
				case 1:
				{
					return 3;
				}

				case 2:
				{
					return 2;
				}

				case 3:
				{
					return 1;
				}

				default:
				{
					return 0;
				}
			}
		}

		private void AlignPosition()
		{
			buf.Position(AlignUp(buf.Position()));
		}

		private void EnsureFreeSpace(int size)
		{
			Preconditions.CheckState(state == XDR.State.Writing);
			if (buf.Remaining() < size)
			{
				int newCapacity = buf.Capacity() * 2;
				int newRemaining = buf.Capacity() + buf.Remaining();
				while (newRemaining < size)
				{
					newRemaining += newCapacity;
					newCapacity *= 2;
				}
				ByteBuffer newbuf = ByteBuffer.Allocate(newCapacity);
				buf.Flip();
				newbuf.Put(buf);
				buf = newbuf;
			}
		}

		/// <summary>check if the rest of data has more than len bytes</summary>
		public static bool VerifyLength(XDR xdr, int len)
		{
			return xdr.buf.Remaining() >= len;
		}

		internal static byte[] RecordMark(int size, bool last)
		{
			byte[] b = new byte[SizeofInt];
			ByteBuffer buf = ByteBuffer.Wrap(b);
			buf.PutInt(!last ? size : size | unchecked((int)(0x80000000)));
			return b;
		}

		/// <summary>Write an XDR message to a TCP ChannelBuffer</summary>
		public static ChannelBuffer WriteMessageTcp(XDR request, bool last)
		{
			Preconditions.CheckState(request.state == XDR.State.Writing);
			ByteBuffer b = request.buf.Duplicate();
			b.Flip();
			byte[] fragmentHeader = XDR.RecordMark(b.Limit(), last);
			ByteBuffer headerBuf = ByteBuffer.Wrap(fragmentHeader);
			// TODO: Investigate whether making a copy of the buffer is necessary.
			return ChannelBuffers.CopiedBuffer(headerBuf, b);
		}

		/// <summary>Write an XDR message to a UDP ChannelBuffer</summary>
		public static ChannelBuffer WriteMessageUdp(XDR response)
		{
			Preconditions.CheckState(response.state == XDR.State.Reading);
			// TODO: Investigate whether making a copy of the buffer is necessary.
			return ChannelBuffers.CopiedBuffer(response.buf);
		}

		public static int FragmentSize(byte[] mark)
		{
			ByteBuffer b = ByteBuffer.Wrap(mark);
			int n = b.GetInt();
			return n & unchecked((int)(0x7fffffff));
		}

		public static bool IsLastFragment(byte[] mark)
		{
			ByteBuffer b = ByteBuffer.Wrap(mark);
			int n = b.GetInt();
			return (n & unchecked((int)(0x80000000))) != 0;
		}

		[VisibleForTesting]
		public byte[] GetBytes()
		{
			ByteBuffer d = AsReadOnlyWrap().Buffer();
			byte[] b = new byte[d.Remaining()];
			d.Get(b);
			return b;
		}
	}
}
