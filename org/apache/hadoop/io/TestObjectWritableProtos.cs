using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Test case for the use of Protocol Buffers within ObjectWritable.</summary>
	public class TestObjectWritableProtos
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testProtoBufs()
		{
			doTest(1);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testProtoBufs2()
		{
			doTest(2);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testProtoBufs3()
		{
			doTest(3);
		}

		/// <summary>
		/// Write a protobuf to a buffer 'numProtos' times, and then
		/// read them back, making sure all data comes through correctly.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void doTest(int numProtos)
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.io.DataOutputBuffer @out = new org.apache.hadoop.io.DataOutputBuffer
				();
			// Write numProtos protobufs to the buffer
			com.google.protobuf.Message[] sent = new com.google.protobuf.Message[numProtos];
			for (int i = 0; i < numProtos; i++)
			{
				// Construct a test protocol buffer using one of the
				// protos that ships with the protobuf library
				com.google.protobuf.Message testProto = ((com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto
					)com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto.newBuilder().setName
					("test" + i).setNumber(i).build());
				org.apache.hadoop.io.ObjectWritable.writeObject(@out, testProto, Sharpen.Runtime.getClassForType
					(typeof(com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto)), conf);
				sent[i] = testProto;
			}
			// Read back the data
			org.apache.hadoop.io.DataInputBuffer @in = new org.apache.hadoop.io.DataInputBuffer
				();
			@in.reset(@out.getData(), @out.getLength());
			for (int i_1 = 0; i_1 < numProtos; i_1++)
			{
				com.google.protobuf.Message received = (com.google.protobuf.Message)org.apache.hadoop.io.ObjectWritable
					.readObject(@in, conf);
				NUnit.Framework.Assert.AreEqual(sent[i_1], received);
			}
		}
	}
}
