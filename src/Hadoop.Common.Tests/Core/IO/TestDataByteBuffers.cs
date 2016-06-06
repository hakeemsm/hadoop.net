using System.IO;
using Hadoop.Common.Core.IO;
using NUnit.Framework;


namespace Org.Apache.Hadoop.IO
{
	public class TestDataByteBuffers
	{
		/// <exception cref="System.IO.IOException"/>
		private static void ReadJunk(BinaryReader reader, Random r, long seed, int iter)
		{
			r.SetSeed(seed);
			for (int i = 0; i < iter; ++i)
			{
				switch (r.Next(7))
				{
					case 0:
					{
						Assert.Equal(unchecked((byte)(r.Next() & unchecked((int)(0xFF)
							))), @in.ReadByte());
						break;
					}

					case 1:
					{
						Assert.Equal((short)(r.Next() & unchecked((int)(0xFFFF))), @in
							.ReadShort());
						break;
					}

					case 2:
					{
						Assert.Equal(r.Next(), @in.ReadInt());
						break;
					}

					case 3:
					{
						Assert.Equal(r.NextLong(), @in.ReadLong());
						break;
					}

					case 4:
					{
						Assert.Equal(double.DoubleToLongBits(r.NextDouble()), double.DoubleToLongBits
							(@in.ReadDouble()));
						break;
					}

					case 5:
					{
						Assert.Equal(Runtime.FloatToIntBits(r.NextFloat()), Runtime.FloatToIntBits
							(@in.ReadFloat()));
						break;
					}

					case 6:
					{
						int len = r.Next(1024);
						byte[] vb = new byte[len];
						r.NextBytes(vb);
						byte[] b = new byte[len];
						@in.ReadFully(b, 0, len);
						Assert.AssertArrayEquals(vb, b);
						break;
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteJunk(BinaryWriter writer, Random r, long seed, int iter)
		{
			r.SetSeed(seed);
			for (int i = 0; i < iter; ++i)
			{
				switch (r.Next(7))
				{
					case 0:
					{
						@out.WriteByte(r.Next());
						break;
					}

					case 1:
					{
						@out.WriteShort((short)(r.Next() & unchecked((int)(0xFFFF))));
						break;
					}

					case 2:
					{
						@out.WriteInt(r.Next());
						break;
					}

					case 3:
					{
						@out.WriteLong(r.NextLong());
						break;
					}

					case 4:
					{
						@out.WriteDouble(r.NextDouble());
						break;
					}

					case 5:
					{
						@out.WriteFloat(r.NextFloat());
						break;
					}

					case 6:
					{
						byte[] b = new byte[r.Next(1024)];
						r.NextBytes(b);
						@out.Write(b);
						break;
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestBaseBuffers()
		{
			DataOutputBuffer dob = new DataOutputBuffer();
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			System.Console.Out.WriteLine("SEED: " + seed);
			WriteJunk(dob, r, seed, 1000);
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(dob.GetData(), 0, dob.GetLength());
			ReadJunk(dib, r, seed, 1000);
			dob.Reset();
			WriteJunk(dob, r, seed, 1000);
			dib.Reset(dob.GetData(), 0, dob.GetLength());
			ReadJunk(dib, r, seed, 1000);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestByteBuffers()
		{
			DataOutputByteBuffer dob = new DataOutputByteBuffer();
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			System.Console.Out.WriteLine("SEED: " + seed);
			WriteJunk(dob, r, seed, 1000);
			DataInputByteBuffer dib = new DataInputByteBuffer();
			dib.Reset(dob.GetData());
			ReadJunk(dib, r, seed, 1000);
			dob.Reset();
			WriteJunk(dob, r, seed, 1000);
			dib.Reset(dob.GetData());
			ReadJunk(dib, r, seed, 1000);
		}

		private static byte[] ToBytes(ByteBuffer[] bufs, int len)
		{
			byte[] ret = new byte[len];
			int pos = 0;
			for (int i = 0; i < bufs.Length; ++i)
			{
				int rem = bufs[i].Remaining();
				bufs[i].Get(ret, pos, rem);
				pos += rem;
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDataOutputByteBufferCompatibility()
		{
			DataOutputBuffer dob = new DataOutputBuffer();
			DataOutputByteBuffer dobb = new DataOutputByteBuffer();
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			System.Console.Out.WriteLine("SEED: " + seed);
			WriteJunk(dob, r, seed, 1000);
			WriteJunk(dobb, r, seed, 1000);
			byte[] check = ToBytes(dobb.GetData(), dobb.GetLength());
			Assert.Equal(check.Length, dob.GetLength());
			Assert.AssertArrayEquals(check, Arrays.CopyOf(dob.GetData(), dob.GetLength()));
			dob.Reset();
			dobb.Reset();
			WriteJunk(dob, r, seed, 3000);
			WriteJunk(dobb, r, seed, 3000);
			check = ToBytes(dobb.GetData(), dobb.GetLength());
			Assert.Equal(check.Length, dob.GetLength());
			Assert.AssertArrayEquals(check, Arrays.CopyOf(dob.GetData(), dob.GetLength()));
			dob.Reset();
			dobb.Reset();
			WriteJunk(dob, r, seed, 1000);
			WriteJunk(dobb, r, seed, 1000);
			check = ToBytes(dobb.GetData(), dobb.GetLength());
			Assert.Equal("Failed Checking length = " + check.Length, check
				.Length, dob.GetLength());
			Assert.AssertArrayEquals(check, Arrays.CopyOf(dob.GetData(), dob.GetLength()));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDataInputByteBufferCompatibility()
		{
			DataOutputBuffer dob = new DataOutputBuffer();
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			System.Console.Out.WriteLine("SEED: " + seed);
			WriteJunk(dob, r, seed, 1000);
			ByteBuffer buf = ByteBuffer.Wrap(dob.GetData(), 0, dob.GetLength());
			DataInputByteBuffer dib = new DataInputByteBuffer();
			dib.Reset(buf);
			ReadJunk(dib, r, seed, 1000);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDataOutputByteBufferCompatibility()
		{
			DataOutputByteBuffer dob = new DataOutputByteBuffer();
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			System.Console.Out.WriteLine("SEED: " + seed);
			WriteJunk(dob, r, seed, 1000);
			ByteBuffer buf = ByteBuffer.Allocate(dob.GetLength());
			foreach (ByteBuffer b in dob.GetData())
			{
				buf.Put(b);
			}
			buf.Flip();
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(((byte[])buf.Array()), 0, buf.Remaining());
			ReadJunk(dib, r, seed, 1000);
		}
	}
}
