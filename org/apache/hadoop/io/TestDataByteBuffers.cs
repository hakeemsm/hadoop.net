using Sharpen;

namespace org.apache.hadoop.io
{
	public class TestDataByteBuffers
	{
		/// <exception cref="System.IO.IOException"/>
		private static void readJunk(java.io.DataInput @in, java.util.Random r, long seed
			, int iter)
		{
			r.setSeed(seed);
			for (int i = 0; i < iter; ++i)
			{
				switch (r.nextInt(7))
				{
					case 0:
					{
						NUnit.Framework.Assert.AreEqual(unchecked((byte)(r.nextInt() & unchecked((int)(0xFF
							)))), @in.readByte());
						break;
					}

					case 1:
					{
						NUnit.Framework.Assert.AreEqual((short)(r.nextInt() & unchecked((int)(0xFFFF))), 
							@in.readShort());
						break;
					}

					case 2:
					{
						NUnit.Framework.Assert.AreEqual(r.nextInt(), @in.readInt());
						break;
					}

					case 3:
					{
						NUnit.Framework.Assert.AreEqual(r.nextLong(), @in.readLong());
						break;
					}

					case 4:
					{
						NUnit.Framework.Assert.AreEqual(double.doubleToLongBits(r.nextDouble()), double.doubleToLongBits
							(@in.readDouble()));
						break;
					}

					case 5:
					{
						NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.floatToIntBits(r.nextFloat()), Sharpen.Runtime.floatToIntBits
							(@in.readFloat()));
						break;
					}

					case 6:
					{
						int len = r.nextInt(1024);
						byte[] vb = new byte[len];
						r.nextBytes(vb);
						byte[] b = new byte[len];
						@in.readFully(b, 0, len);
						NUnit.Framework.Assert.assertArrayEquals(vb, b);
						break;
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void writeJunk(java.io.DataOutput @out, java.util.Random r, long seed
			, int iter)
		{
			r.setSeed(seed);
			for (int i = 0; i < iter; ++i)
			{
				switch (r.nextInt(7))
				{
					case 0:
					{
						@out.writeByte(r.nextInt());
						break;
					}

					case 1:
					{
						@out.writeShort((short)(r.nextInt() & unchecked((int)(0xFFFF))));
						break;
					}

					case 2:
					{
						@out.writeInt(r.nextInt());
						break;
					}

					case 3:
					{
						@out.writeLong(r.nextLong());
						break;
					}

					case 4:
					{
						@out.writeDouble(r.nextDouble());
						break;
					}

					case 5:
					{
						@out.writeFloat(r.nextFloat());
						break;
					}

					case 6:
					{
						byte[] b = new byte[r.nextInt(1024)];
						r.nextBytes(b);
						@out.write(b);
						break;
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testBaseBuffers()
		{
			org.apache.hadoop.io.DataOutputBuffer dob = new org.apache.hadoop.io.DataOutputBuffer
				();
			java.util.Random r = new java.util.Random();
			long seed = r.nextLong();
			r.setSeed(seed);
			System.Console.Out.WriteLine("SEED: " + seed);
			writeJunk(dob, r, seed, 1000);
			org.apache.hadoop.io.DataInputBuffer dib = new org.apache.hadoop.io.DataInputBuffer
				();
			dib.reset(dob.getData(), 0, dob.getLength());
			readJunk(dib, r, seed, 1000);
			dob.reset();
			writeJunk(dob, r, seed, 1000);
			dib.reset(dob.getData(), 0, dob.getLength());
			readJunk(dib, r, seed, 1000);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testByteBuffers()
		{
			org.apache.hadoop.io.DataOutputByteBuffer dob = new org.apache.hadoop.io.DataOutputByteBuffer
				();
			java.util.Random r = new java.util.Random();
			long seed = r.nextLong();
			r.setSeed(seed);
			System.Console.Out.WriteLine("SEED: " + seed);
			writeJunk(dob, r, seed, 1000);
			org.apache.hadoop.io.DataInputByteBuffer dib = new org.apache.hadoop.io.DataInputByteBuffer
				();
			dib.reset(dob.getData());
			readJunk(dib, r, seed, 1000);
			dob.reset();
			writeJunk(dob, r, seed, 1000);
			dib.reset(dob.getData());
			readJunk(dib, r, seed, 1000);
		}

		private static byte[] toBytes(java.nio.ByteBuffer[] bufs, int len)
		{
			byte[] ret = new byte[len];
			int pos = 0;
			for (int i = 0; i < bufs.Length; ++i)
			{
				int rem = bufs[i].remaining();
				bufs[i].get(ret, pos, rem);
				pos += rem;
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDataOutputByteBufferCompatibility()
		{
			org.apache.hadoop.io.DataOutputBuffer dob = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.DataOutputByteBuffer dobb = new org.apache.hadoop.io.DataOutputByteBuffer
				();
			java.util.Random r = new java.util.Random();
			long seed = r.nextLong();
			r.setSeed(seed);
			System.Console.Out.WriteLine("SEED: " + seed);
			writeJunk(dob, r, seed, 1000);
			writeJunk(dobb, r, seed, 1000);
			byte[] check = toBytes(dobb.getData(), dobb.getLength());
			NUnit.Framework.Assert.AreEqual(check.Length, dob.getLength());
			NUnit.Framework.Assert.assertArrayEquals(check, java.util.Arrays.copyOf(dob.getData
				(), dob.getLength()));
			dob.reset();
			dobb.reset();
			writeJunk(dob, r, seed, 3000);
			writeJunk(dobb, r, seed, 3000);
			check = toBytes(dobb.getData(), dobb.getLength());
			NUnit.Framework.Assert.AreEqual(check.Length, dob.getLength());
			NUnit.Framework.Assert.assertArrayEquals(check, java.util.Arrays.copyOf(dob.getData
				(), dob.getLength()));
			dob.reset();
			dobb.reset();
			writeJunk(dob, r, seed, 1000);
			writeJunk(dobb, r, seed, 1000);
			check = toBytes(dobb.getData(), dobb.getLength());
			NUnit.Framework.Assert.AreEqual("Failed Checking length = " + check.Length, check
				.Length, dob.getLength());
			NUnit.Framework.Assert.assertArrayEquals(check, java.util.Arrays.copyOf(dob.getData
				(), dob.getLength()));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDataInputByteBufferCompatibility()
		{
			org.apache.hadoop.io.DataOutputBuffer dob = new org.apache.hadoop.io.DataOutputBuffer
				();
			java.util.Random r = new java.util.Random();
			long seed = r.nextLong();
			r.setSeed(seed);
			System.Console.Out.WriteLine("SEED: " + seed);
			writeJunk(dob, r, seed, 1000);
			java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(dob.getData(), 0, dob.getLength
				());
			org.apache.hadoop.io.DataInputByteBuffer dib = new org.apache.hadoop.io.DataInputByteBuffer
				();
			dib.reset(buf);
			readJunk(dib, r, seed, 1000);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDataOutputByteBufferCompatibility()
		{
			org.apache.hadoop.io.DataOutputByteBuffer dob = new org.apache.hadoop.io.DataOutputByteBuffer
				();
			java.util.Random r = new java.util.Random();
			long seed = r.nextLong();
			r.setSeed(seed);
			System.Console.Out.WriteLine("SEED: " + seed);
			writeJunk(dob, r, seed, 1000);
			java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(dob.getLength());
			foreach (java.nio.ByteBuffer b in dob.getData())
			{
				buf.put(b);
			}
			buf.flip();
			org.apache.hadoop.io.DataInputBuffer dib = new org.apache.hadoop.io.DataInputBuffer
				();
			dib.reset(((byte[])buf.array()), 0, buf.remaining());
			readJunk(dib, r, seed, 1000);
		}
	}
}
