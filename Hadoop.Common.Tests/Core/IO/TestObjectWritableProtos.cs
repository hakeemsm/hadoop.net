using Com.Google.Protobuf;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>Test case for the use of Protocol Buffers within ObjectWritable.</summary>
	public class TestObjectWritableProtos
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestProtoBufs()
		{
			DoTest(1);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestProtoBufs2()
		{
			DoTest(2);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestProtoBufs3()
		{
			DoTest(3);
		}

		/// <summary>
		/// Write a protobuf to a buffer 'numProtos' times, and then
		/// read them back, making sure all data comes through correctly.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void DoTest(int numProtos)
		{
			Configuration conf = new Configuration();
			DataOutputBuffer @out = new DataOutputBuffer();
			// Write numProtos protobufs to the buffer
			Message[] sent = new Message[numProtos];
			for (int i = 0; i < numProtos; i++)
			{
				// Construct a test protocol buffer using one of the
				// protos that ships with the protobuf library
				Message testProto = ((DescriptorProtos.EnumValueDescriptorProto)DescriptorProtos.EnumValueDescriptorProto
					.NewBuilder().SetName("test" + i).SetNumber(i).Build());
				ObjectWritable.WriteObject(@out, testProto, typeof(DescriptorProtos.EnumValueDescriptorProto
					), conf);
				sent[i] = testProto;
			}
			// Read back the data
			DataInputBuffer @in = new DataInputBuffer();
			@in.Reset(@out.GetData(), @out.GetLength());
			for (int i_1 = 0; i_1 < numProtos; i_1++)
			{
				Message received = (Message)ObjectWritable.ReadObject(@in, conf);
				NUnit.Framework.Assert.AreEqual(sent[i_1], received);
			}
		}
	}
}
